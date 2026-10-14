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

            Instant sentAt = Instant.parse(matcher.group(1));

            double latencyMs = Duration.between(sentAt, receivedAt)
                    .toNanos() / 1_000_000.0;

            pushLatenciesMs.add(latencyMs);

        } catch (RuntimeException error) {
            invalidMessages.increment();
        }
    }

    private final class SubscriberListener implements WebSocket.Listener {

        private final long startNanos;
        private final StringBuilder fragments = new StringBuilder();

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
        public CompletionStage<?> onClose(WebSocket webSocket,
                                          int statusCode,
                                          String reason) {
            if (socket == webSocket) {
                socket = null;
                scheduleReconnect();
            }

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
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