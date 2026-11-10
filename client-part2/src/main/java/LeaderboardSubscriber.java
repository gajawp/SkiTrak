import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledFuture;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LeaderboardSubscriber implements AutoCloseable {

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
            "\"serverTimestamp\"\\s*:\\s*\"([^\"]+)\"");

    private final HttpClient client;
    private final URI endpoint;
    private final int resortID;

    private final ScheduledExecutorService reconnectExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);

    private final LongAdder updates = new LongAdder();
    private final LongAdder connections = new LongAdder();
    private final LongAdder connectionFailures = new LongAdder();
    private final LongAdder invalidMessages = new LongAdder();

    private final ConcurrentLinkedQueue<Double> pushLatenciesMs =
            new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<Double> connectionTimesMs =
            new ConcurrentLinkedQueue<>();

    private volatile WebSocket socket;

    public LeaderboardSubscriber(HttpClient client,
                                 String baseUrl,
                                 int resortID) {

        if (client == null || baseUrl == null || resortID <= 0) {
            throw new IllegalArgumentException(
                    "A client, base URL, and positive resort ID are required");
        }

        this.client = client;
        this.resortID = resortID;

        URI base = URI.create(baseUrl);

        String scheme;
        if ("http".equalsIgnoreCase(base.getScheme())) {
            scheme = "ws";
        } else if ("https".equalsIgnoreCase(base.getScheme())) {
            scheme = "wss";
        } else {
            throw new IllegalArgumentException(
                    "Base URL must use http or https");
        }

        String path = base.getRawPath();
        if (path == null) {
            path = "";
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        this.endpoint = URI.create(
                scheme + "://" + base.getRawAuthority()
                        + path + "/leaderboard/" + resortID);

        this.reconnectExecutor =
                Executors.newSingleThreadScheduledExecutor(task -> {
                    Thread thread = new Thread(
                            task, "leaderboard-reconnect-" + resortID);
                    thread.setDaemon(true);
                    return thread;
                });
    }

    public CompletableFuture<WebSocket> start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "Subscriber is already running");
        }

        return connect();
    }

    private CompletableFuture<WebSocket> connect() {
        long startNanos = System.nanoTime();

        CompletableFuture<WebSocket> future = client.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(endpoint, new SubscriberListener(startNanos));

        future.whenComplete((webSocket, error) -> {
            if (error != null && running.get()) {
                log("connection failed: " + error);
                connectionFailures.increment();
                scheduleReconnect();
            }
        });

        return future;
    }

    private void scheduleReconnect() {
        if (!running.get()
                || !reconnectScheduled.compareAndSet(false, true)) {
            return;
        }

        try {
            reconnectExecutor.schedule(() -> {
                reconnectScheduled.set(false);

                if (running.get()) {
                    connect();
                }
            }, 1, TimeUnit.SECONDS);
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            reconnectScheduled.set(false);
            // The subscriber was closed while scheduling.
        }
    }

    private void recordMessage(String message, Instant receivedAt) {
        updates.increment();

        try {
            Matcher matcher = TIMESTAMP_PATTERN.matcher(message);

            if (!matcher.find()) {
                invalidMessages.increment();
                return;
            }

            Instant.parse(matcher.group(1));

            // Cross-machine timestamps are not reliable latency measurements.
            // Keep counting updates, but do not collect these latency samples.

        } catch (RuntimeException error) {
            invalidMessages.increment();
        }
    }

    private void log(String message) {
        System.err.printf("%s [WebSocket resort %d] %s%n",
                Instant.now(), resortID, message);
    }

    private final class SubscriberListener implements WebSocket.Listener {

        private final long startNanos;
        private final StringBuilder fragments = new StringBuilder();
        private final AtomicBoolean pingInFlight = new AtomicBoolean(false);
        private volatile long lastPongNanos;
        private volatile ScheduledFuture<?> heartbeat;

        private void stopHeartbeat() {
            ScheduledFuture<?> task = heartbeat;
            if (task != null) task.cancel(false);
        }

        private void failConnection(WebSocket webSocket, String message) {
            stopHeartbeat();
            if (running.get() && socket == webSocket) {
                log(message);
                socket = null;
                webSocket.abort();
                scheduleReconnect();
            }
        }

        private void sendHeartbeat(WebSocket webSocket) {
            if (!running.get() || socket != webSocket) {
                stopHeartbeat();
                return;
            }
            if (System.nanoTime() - lastPongNanos > TimeUnit.SECONDS.toNanos(45)) {
                failConnection(webSocket, "heartbeat timeout: no pong for 45 seconds");
                return;
            }
            if (!pingInFlight.compareAndSet(false, true)) return;
            ByteBuffer payload = ByteBuffer.allocate(Long.BYTES);
            payload.putLong(System.nanoTime()).flip();
            try {
                webSocket.sendPing(payload).orTimeout(10, TimeUnit.SECONDS)
                        .whenComplete((ignored, error) -> {
                            pingInFlight.set(false);
                            if (error != null) {
                                failConnection(webSocket, "ping failed: " + error);
                            }
                        });
            } catch (RuntimeException error) {
                pingInFlight.set(false);
                failConnection(webSocket, "ping failed: " + error);
            }
        }

        private SubscriberListener(long startNanos) {
            this.startNanos = startNanos;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            if (!running.get()) {
                webSocket.abort();
                return;
            }

            socket = webSocket;
            connections.increment();

            connectionTimesMs.add(
                    (System.nanoTime() - startNanos) / 1_000_000.0);

            lastPongNanos = System.nanoTime();
            try {
                heartbeat = reconnectExecutor.scheduleAtFixedRate(
                        () -> sendHeartbeat(webSocket), 20, 20, TimeUnit.SECONDS);
                if (!running.get()) stopHeartbeat();
            } catch (java.util.concurrent.RejectedExecutionException error) {
                webSocket.abort();
                return;
            }

            // Request the first incoming message.
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket,
                                         CharSequence data,
                                         boolean last) {

            Instant receivedAt = Instant.now();
            fragments.append(data);

            // A JSON message can arrive in multiple fragments.
            if (last) {
                if (running.get()) {
                    recordMessage(fragments.toString(), receivedAt);
                }
                fragments.setLength(0);
            }

            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            lastPongNanos = System.nanoTime();
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket,
                                          int statusCode,
                                          String reason) {
            stopHeartbeat();
            if (running.get()) {
                log("closed: code=" + statusCode + ", reason=" + reason);
            }
            if (socket == webSocket) {
                socket = null;
                scheduleReconnect();
            }

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            stopHeartbeat();
            if (running.get()) log("error: " + error);
            webSocket.abort();

            if (socket == webSocket) {
                socket = null;
                scheduleReconnect();
            }
        }
    }

    public int getResortID() {
        return resortID;
    }

    public long getUpdates() {
        return updates.sum();
    }

    public long getConnections() {
        return connections.sum();
    }

    public long getReconnections() {
        return Math.max(0, connections.sum() - 1);
    }

    public long getConnectionFailures() {
        return connectionFailures.sum();
    }

    public long getInvalidMessages() {
        return invalidMessages.sum();
    }

    public double[] getPushLatenciesMs() {
        return pushLatenciesMs.stream()
                .mapToDouble(Double::doubleValue)
                .toArray();
    }

    public double[] getConnectionTimesMs() {
        return connectionTimesMs.stream()
                .mapToDouble(Double::doubleValue)
                .toArray();
    }

    @Override
    public void close() {
        running.set(false);
        reconnectExecutor.shutdownNow();

        WebSocket current = socket;
        socket = null;

        if (current != null) {
            current.sendClose(WebSocket.NORMAL_CLOSURE, "Run complete")
                    .orTimeout(2, TimeUnit.SECONDS)
                    .whenComplete((ignored, error) -> current.abort());
        }
    }
}