package messaging;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/** Bounded streaming confirms; one sending thread owns each channel.
 * HTTP callers still wait for their own mandatory, persistent publish confirmation.
 */
public final class ChannelPool implements AutoCloseable, SwipePublisher {
    private static final Logger LOG = Logger.getLogger(ChannelPool.class.getName());
    private final ConnectionFactory factory;
    private final int size;
    private final int maxInFlight;
    private final long admissionMillis;
    private final long confirmMillis;
    private final ScheduledExecutorService repair = Executors.newSingleThreadScheduledExecutor();
    private volatile Generation current;
    private volatile boolean closed;

    private static final class Pending {
        final String attemptId = UUID.randomUUID().toString();
        final CompletableFuture<Void> done = new CompletableFuture<>();
        volatile long sequence;
        volatile boolean returned;
    }

    private final class Slot {
        final Generation generation;
        final Channel channel;
        final Semaphore capacity = new Semaphore(maxInFlight);
        final ConcurrentSkipListMap<Long, Pending> bySeq = new ConcurrentSkipListMap<>();
        final ConcurrentHashMap<String, Pending> byAttempt = new ConcurrentHashMap<>();
        final ThreadPoolExecutor sender;

        Slot(Generation generation, Channel channel) {
            this.generation = generation;
            this.channel = channel;
            sender = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(maxInFlight), runnable -> {
                Thread thread = new Thread(runnable, "skitrak-publish-lane");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
            channel.addReturnListener((code, text, exchange, key, props, body) -> {
                Pending p = props == null ? null : byAttempt.get(props.getCorrelationId());
                if (p != null) p.returned = true;
            });
            channel.addConfirmListener(new ConfirmListener() {
                @Override public void handleAck(long tag, boolean multiple) {
                    settle(tag, multiple, null);
                }
                @Override public void handleNack(long tag, boolean multiple) {
                    settle(tag, multiple, new IOException("Broker nacked publish"));
                }
            });
            channel.addShutdownListener(cause -> invalidate(generation,
                    new IOException("Publisher channel closed", cause)));
        }

        void settle(long tag, boolean multiple, IOException failure) {
            if (multiple) {
                while (true) {
                    java.util.Map.Entry<Long, Pending> first = bySeq.firstEntry();
                    if (first == null || first.getKey() > tag) return;
                    if (bySeq.remove(first.getKey(), first.getValue())) finish(first.getValue(), failure);
                }
            } else {
                Pending p = bySeq.remove(tag);
                if (p != null) finish(p, failure);
            }
        }

        void finish(Pending p, IOException failure) {
            if (failure != null) p.done.completeExceptionally(failure);
            else if (p.returned) p.done.completeExceptionally(new IOException("Unroutable swipe"));
            else if (closed || !generation.valid.get() || current != generation)
                p.done.completeExceptionally(new IOException("Publisher generation changed"));
            else p.done.complete(null);
        }

        void failAll(IOException failure) {
            byAttempt.forEach((id, p) -> p.done.completeExceptionally(failure));
        }

        void enqueue(Pending pending, String eventId, String routingKey, byte[] body) {
            sender.execute(() -> {
                try {
                    if (closed || current != generation || !generation.valid.get()
                            || pending.done.isDone() || !channel.isOpen()) {
                        pending.done.completeExceptionally(new IOException("Publisher generation changed"));
                        return;
                    }
                    long seq = channel.getNextPublishSeqNo();
                    pending.sequence = seq;
                    bySeq.put(seq, pending);
                    // A timeout/close may have completed the future during registration.
                    if (pending.done.isDone() || !generation.valid.get() || current != generation) {
                        bySeq.remove(seq, pending);
                        pending.done.completeExceptionally(new IOException("Publisher generation changed"));
                        return;
                    }
                    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                            .contentType("application/json").deliveryMode(2)
                            .messageId(eventId).correlationId(pending.attemptId).build();
                    channel.basicPublish(Topology.EXCHANGE, routingKey, true, props, body);
                } catch (IOException | RuntimeException error) {
                    invalidate(generation, new IOException("Publish outcome uncertain", error));
                }
            });
        }
    }

    private final class Generation {
        final Connection connection;
        final List<Slot> slots = new CopyOnWriteArrayList<>();
        final AtomicInteger next = new AtomicInteger();
        final AtomicBoolean valid = new AtomicBoolean(true);
        Generation(Connection connection) { this.connection = connection; }
        boolean healthy() {
            return valid.get() && connection.isOpen() && slots.size() == size
                    && slots.stream().allMatch(s -> s.channel.isOpen());
        }
        void stop() {
            valid.set(false);
            for (Slot slot : slots) {
                slot.sender.shutdownNow();
                slot.failAll(new IOException("Publisher generation stopped"));
            }
            try { connection.abort(); } catch (Exception ignored) { }
        }
    }

    public ChannelPool(ConnectionFactory factory, int size) {
        this(factory, size, 64, 1000, 3000);
    }

    // Package-visible configuration for deterministic failure-path tests.
    ChannelPool(ConnectionFactory factory, int size, int maxInFlight,
                long admissionMillis, long confirmMillis) {
        if (factory == null || size <= 0 || maxInFlight <= 0
                || admissionMillis <= 0 || confirmMillis <= 0)
            throw new IllegalArgumentException("Invalid publisher configuration");
        this.factory = factory;
        this.size = size;
        this.maxInFlight = maxInFlight;
        this.admissionMillis = admissionMillis;
        this.confirmMillis = confirmMillis;
        repair.scheduleWithFixedDelay(this::reconnect, 0, 2, TimeUnit.SECONDS);
    }

    private void invalidate(Generation g, IOException failure) {
        if (!g.valid.compareAndSet(true, false)) return;
        for (Slot slot : g.slots) slot.failAll(failure);
        // Never perform blocking channel/connection operations on a confirm callback.
        try { repair.execute(g::stop); }
        catch (RejectedExecutionException ignored) { /* close() owns shutdown */ }
    }

    private synchronized void reconnect() {
        if (closed || (current != null && current.healthy())) return;
        Generation old = current;
        current = null;
        if (old != null) old.stop();
        Generation replacement = null;
        Connection connection = null;
        try {
            connection = factory.newConnection("skitrak-ingest");
            replacement = new Generation(connection);
            for (int i = 0; i < size; i++) {
                Channel channel = connection.createChannel();
                if (channel == null) throw new IOException("Channel limit reached");
                if (i == 0) Topology.declare(channel);
                channel.confirmSelect();
                replacement.slots.add(new Slot(replacement, channel));
            }
            if (!replacement.healthy()) throw new IOException("Connection closed during setup");
            current = replacement;
            LOG.info("RabbitMQ streaming confirms ready; channels=" + size
                    + ", maxInFlightPerChannel=" + maxInFlight);
        } catch (Exception error) {
            if (replacement != null) replacement.stop();
            else if (connection != null) try { connection.abort(); } catch (Exception ignored) { }
            LOG.warning("RabbitMQ unavailable; reconnect scheduled: " + error);
        }
    }

    public boolean ready() {
        Generation g = current;
        return !closed && g != null && g.healthy();
    }

    @Override public void publish(String eventId, String routingKey, byte[] body)
            throws IOException, InterruptedException {
        Generation g = current;
        if (closed || g == null || !g.healthy()) throw new IOException("Broker unavailable");
        // Try all lanes before waiting on one, avoiding an unnecessary hot lane.
        int start = Math.floorMod(g.next.getAndIncrement(), g.slots.size());
        Slot selected = null;
        for (int i = 0; i < g.slots.size(); i++) {
            Slot candidate = g.slots.get((start + i) % g.slots.size());
            if (candidate.capacity.tryAcquire()) { selected = candidate; break; }
        }
        if (selected == null) {
            selected = g.slots.get(start);
            if (!selected.capacity.tryAcquire(admissionMillis, TimeUnit.MILLISECONDS))
                throw new IOException("Publisher in-flight capacity busy");
        }
        final Slot slot = selected;
        Pending pending = new Pending();
        slot.byAttempt.put(pending.attemptId, pending);
        pending.done.whenComplete((ignored, error) -> {
            slot.byAttempt.remove(pending.attemptId, pending);
            slot.bySeq.remove(pending.sequence, pending);
            slot.capacity.release();
        });
        if (closed || current != g || !g.valid.get()) {
            pending.done.completeExceptionally(new IOException("Publisher generation changed"));
        } else {
            try { slot.enqueue(pending, eventId, routingKey, body); }
            catch (RejectedExecutionException error) {
                pending.done.completeExceptionally(new IOException("Publisher lane unavailable", error));
            }
        }
        try {
            // Includes sender queueing and a blocked basicPublish, not just broker confirmation.
            pending.done.get(confirmMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException error) {
            invalidate(g, new IOException("Publish deadline exceeded; outcome uncertain", error));
            pending.done.completeExceptionally(new IOException("Publish deadline exceeded", error));
            throw new IOException("Publish deadline exceeded; retry same event ID", error);
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException) throw (IOException) cause;
            throw new IOException("Publish failed", cause);
        } catch (InterruptedException error) {
            invalidate(g, new IOException("Publish interrupted; outcome uncertain", error));
            pending.done.completeExceptionally(error);
            throw error;
        }
    }

    @Override public synchronized void close() {
        closed = true;
        repair.shutdownNow();
        Generation g = current;
        current = null;
        if (g != null) g.stop();
    }
}
