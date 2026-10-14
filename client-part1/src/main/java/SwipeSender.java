import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.LongAdder;

public final class SwipeSender implements Runnable {

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_BACKOFF_MS = 100;

    private final HttpClient client;
    private final BlockingQueue<SwipeEvent> queue;
    private final String baseUrl;
    private final int swipeLimit;
    private final Metrics metrics;

    public SwipeSender(HttpClient client,
                       BlockingQueue<SwipeEvent> queue,
                       String baseUrl,
                       int swipeLimit,
                       Metrics metrics) {

        if (client == null || queue == null || metrics == null) {
            throw new IllegalArgumentException(
                    "client, queue, and metrics cannot be null");
        }
        if (baseUrl == null || baseUrl.isEmpty() || swipeLimit <= 0) {
            throw new IllegalArgumentException(
                    "A base URL and positive swipe limit are required");
        }

        this.client = client;
        this.queue = queue;
        this.baseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        this.swipeLimit = swipeLimit;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        for (int i = 0; i < swipeLimit; i++) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }

            // Generation is already complete, so polling never waits.
            SwipeEvent event = queue.poll();

            if (event == null) {
                metrics.queueUnderflows.increment();
                return;
            }

            try {
                sendWithRetries(event);
            } catch (InterruptedException error) {
                metrics.failed.increment();
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException error) {
                // Count this event as failed and continue with the next.
                metrics.failed.increment();
            }
        }
    }

    private void sendWithRetries(SwipeEvent event)
            throws InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + event.getPath()))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        event.getJsonBody()))
                .build();

        // One initial attempt, followed by up to five retries.
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            if (attempt > 0) {
                long backoffMs =
                        INITIAL_BACKOFF_MS * (1L << (attempt - 1));

                Thread.sleep(backoffMs);
                metrics.retries.increment();
            }

            metrics.httpAttempts.increment();

            try {
                HttpResponse<Void> response = client.send(
                        request,
                        HttpResponse.BodyHandlers.discarding());

                if (response.statusCode() == 201) {
                    metrics.successful.increment();
                    return;
                }

                metrics.non201Responses.increment();

            } catch (IOException error) {
                metrics.ioErrors.increment();
            }
        }

        metrics.failed.increment();
    }

    public static final class Metrics {

        private final LongAdder successful = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final LongAdder httpAttempts = new LongAdder();
        private final LongAdder retries = new LongAdder();
        private final LongAdder non201Responses = new LongAdder();
        private final LongAdder ioErrors = new LongAdder();
        private final LongAdder queueUnderflows = new LongAdder();

        public long getSuccessful() {
            return successful.sum();
        }

        public long getFailed() {
            return failed.sum();
        }

        public long getHttpAttempts() {
            return httpAttempts.sum();
        }

        public long getRetries() {
            return retries.sum();
        }

        public long getNon201Responses() {
            return non201Responses.sum();
        }

        public long getIoErrors() {
            return ioErrors.sum();
        }

        public long getQueueUnderflows() {
            return queueUnderflows.sum();
        }
    }
}