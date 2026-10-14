import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class LoadClient {

    private static final int TOTAL_SWIPES = 500_000;
    private static final int WARMUP_THREADS = 32;
    private static final int WARMUP_SWIPES_PER_THREAD = 1_000;
    private static final int WARMUP_SWIPES =
            WARMUP_THREADS * WARMUP_SWIPES_PER_THREAD;

    public static void main(String[] args) throws Exception {
        String baseUrl = args.length > 0
                ? args[0]
                : "http://localhost:8080/ski-servlet";

        int mainThreads = args.length > 1
                ? Integer.parseInt(args[1]) : 32;

        int resortCount = args.length > 2
                ? Integer.parseInt(args[2]) : 10;

        int seasonID = args.length > 3
                ? Integer.parseInt(args[3]) : 2026;

        int dayID = args.length > 4
                ? Integer.parseInt(args[4]) : 1;

        if (mainThreads <= 0 || resortCount <= 0
                || seasonID <= 0 || dayID <= 0) {
            throw new IllegalArgumentException(
                    "Thread count, resort count, season, and day must be positive");
        }

        System.out.println("=== SkiTrak Client Part 1 ===");
        System.out.println("Target: " + baseUrl);
        System.out.println("Total swipes: " + TOTAL_SWIPES);
        System.out.println("Warmup threads: " + WARMUP_THREADS);
        System.out.println("Main threads: " + mainThreads);
        System.out.println("Resorts / subscribers: " + resortCount);

        long programStart = System.nanoTime();

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        BlockingQueue<SwipeEvent> queue =
                new ArrayBlockingQueue<>(TOTAL_SWIPES);

        // Generate all events on one dedicated thread before sending.
        long generationStart = System.nanoTime();

        SwipeGenerator generator = new SwipeGenerator(
                queue, TOTAL_SWIPES, resortCount, seasonID, dayID);

        Thread generatorThread = new Thread(generator, "swipe-generator");
        generatorThread.start();
        generatorThread.join();
        generator.checkCompleted();

        double generationSeconds = secondsSince(generationStart);

        System.out.printf(
                "%nGenerated %,d swipes in %.3f seconds%n",
                generator.getGeneratedCount(), generationSeconds);

        List<LeaderboardSubscriber> subscribers = new ArrayList<>();
        List<CompletableFuture<?>> connectionFutures = new ArrayList<>();

        SwipeSender.Metrics warmupMetrics = new SwipeSender.Metrics();
        SwipeSender.Metrics mainMetrics = new SwipeSender.Metrics();

        double warmupSeconds;
        double mainSeconds;
        double ingestSeconds;

        try {
            System.out.println("Connecting leaderboard subscribers...");

            for (int resort = 1; resort <= resortCount; resort++) {
                LeaderboardSubscriber subscriber =
                        new LeaderboardSubscriber(client, baseUrl, resort);

                subscribers.add(subscriber);
                connectionFutures.add(subscriber.start());
            }

            // Require all initial subscriptions before starting ingest.
            CompletableFuture.allOf(
                            connectionFutures.toArray(new CompletableFuture<?>[0]))
                    .get(20, TimeUnit.SECONDS);

            System.out.println("All leaderboard subscribers connected.");

            long ingestStart = System.nanoTime();

            System.out.printf(
                    "%nStarting warmup: %d threads, %,d swipes%n",
                    WARMUP_THREADS, WARMUP_SWIPES);

            warmupSeconds = runPhase(
                    "warmup",
                    client,
                    queue,
                    baseUrl,
                    WARMUP_THREADS,
                    WARMUP_SWIPES,
                    warmupMetrics);

            System.out.printf(
                    "Warmup complete: %.3f seconds%n", warmupSeconds);

            int mainSwipes = TOTAL_SWIPES - WARMUP_SWIPES;

            System.out.printf(
                    "%nStarting main phase: %d threads, %,d swipes%n",
                    mainThreads, mainSwipes);

            mainSeconds = runPhase(
                    "main",
                    client,
                    queue,
                    baseUrl,
                    mainThreads,
                    mainSwipes,
                    mainMetrics);

            ingestSeconds = secondsSince(ingestStart);

        } finally {
            for (LeaderboardSubscriber subscriber : subscribers) {
                subscriber.close();
            }
        }

        double totalSeconds = secondsSince(programStart);

        printPhase("Warmup", warmupMetrics, warmupSeconds);
        printPhase("Main", mainMetrics, mainSeconds);

        long successful = warmupMetrics.getSuccessful()
                + mainMetrics.getSuccessful();

        long failed = warmupMetrics.getFailed()
                + mainMetrics.getFailed();

        long attempts = warmupMetrics.getHttpAttempts()
                + mainMetrics.getHttpAttempts();

        long retries = warmupMetrics.getRetries()
                + mainMetrics.getRetries();

        long underflows = warmupMetrics.getQueueUnderflows()
                + mainMetrics.getQueueUnderflows();

        System.out.println("\n=== Overall Ingest Results ===");
        System.out.printf("Successful swipes: %,d%n", successful);
        System.out.printf("Failed swipes: %,d%n", failed);
        System.out.printf("HTTP attempts: %,d%n", attempts);
        System.out.printf("Retry attempts: %,d%n", retries);
        System.out.printf("Queue underflows: %,d%n", underflows);
        System.out.printf("Events left in queue: %,d%n", queue.size());
        System.out.printf("Generation time: %.3f seconds%n",
                generationSeconds);
        System.out.printf("Warmup time: %.3f seconds%n", warmupSeconds);
        System.out.printf("Main phase time: %.3f seconds%n", mainSeconds);
        System.out.printf("Ingest runtime: %.3f seconds%n", ingestSeconds);
        System.out.printf("Total wall-clock runtime: %.3f seconds%n",
                totalSeconds);

        System.out.printf(
                "Ingest throughput, including warmup: %,.2f swipes/sec%n",
                successful / ingestSeconds);

        System.out.printf(
                "End-to-end throughput, including setup: %,.2f swipes/sec%n",
                successful / totalSeconds);

        System.out.println(
                "HTTP: one shared HttpClient; connections reused internally.");
        System.out.println(
                "Physical HTTP connection counts: not exposed by this client API.");

        printSubscribers(subscribers);

        if (successful + failed != TOTAL_SWIPES
                || underflows != 0 || !queue.isEmpty()) {
            throw new IllegalStateException(
                    "Incomplete run: swipe accounting does not match "
                            + TOTAL_SWIPES);
        }

        System.out.println("\nRUN COMPLETE: All 500,000 swipes accounted for.");
    }

    private static double runPhase(
            String phase,
            HttpClient client,
            BlockingQueue<SwipeEvent> queue,
            String baseUrl,
            int threadCount,
            int swipeCount,
            SwipeSender.Metrics metrics) throws InterruptedException {

        Thread[] workers = new Thread[threadCount];

        int perThread = swipeCount / threadCount;
        int remainder = swipeCount % threadCount;

        // Construct workers before timing their execution.
        for (int i = 0; i < threadCount; i++) {
            int assignedSwipes = perThread + (i < remainder ? 1 : 0);

            if (assignedSwipes > 0) {
                workers[i] = new Thread(
                        new SwipeSender(
                                client, queue, baseUrl, assignedSwipes, metrics),
                        phase + "-sender-" + (i + 1));
            }
        }

        long start = System.nanoTime();

        try {
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.start();
                }
            }

            for (Thread worker : workers) {
                if (worker != null) {
                    worker.join();
                }
            }
        } catch (InterruptedException error) {
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.interrupt();
                }
            }

            // Wait for workers to stop before unwinding the run.
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.join();
                }
            }

            Thread.currentThread().interrupt();
            throw error;
        }

        return secondsSince(start);
    }

    private static void printPhase(
            String name,
            SwipeSender.Metrics metrics,
            double seconds) {

        System.out.println("\n=== " + name + " Results ===");
        System.out.printf("Successful swipes: %,d%n",
                metrics.getSuccessful());
        System.out.printf("Failed swipes: %,d%n", metrics.getFailed());
        System.out.printf("HTTP attempts: %,d%n", metrics.getHttpAttempts());
        System.out.printf("Retries: %,d%n", metrics.getRetries());
        System.out.printf("Non-201 responses: %,d%n",
                metrics.getNon201Responses());
        System.out.printf("I/O errors: %,d%n", metrics.getIoErrors());
        System.out.printf("Runtime: %.3f seconds%n", seconds);
        System.out.printf("Throughput: %,.2f swipes/sec%n",
                metrics.getSuccessful() / seconds);
    }

    private static void printSubscribers(
            List<LeaderboardSubscriber> subscribers) {

        System.out.println("\n=== WebSocket Results ===");

        long totalUpdates = 0;
        long totalConnections = 0;
        long totalReconnections = 0;

        List<double[]> latencySamples = new ArrayList<>();

        for (LeaderboardSubscriber subscriber : subscribers) {
            totalUpdates += subscriber.getUpdates();
            totalConnections += subscriber.getConnections();
            totalReconnections += subscriber.getReconnections();
            latencySamples.add(subscriber.getPushLatenciesMs());

            double[] connectionTimes = subscriber.getConnectionTimesMs();

            System.out.printf(
                    "Resort %d: updates=%,d, connections=%d, "
                            + "reconnections=%d, connection failures=%d, "
                            + "invalid messages=%d%n",
                    subscriber.getResortID(),
                    subscriber.getUpdates(),
                    subscriber.getConnections(),
                    subscriber.getReconnections(),
                    subscriber.getConnectionFailures(),
                    subscriber.getInvalidMessages());

            if (connectionTimes.length > 0) {
                System.out.printf(
                        "  Initial connection establishment: %.3f ms%n",
                        connectionTimes[0]);
            }
        }

        System.out.printf("Total leaderboard updates: %,d%n", totalUpdates);
        System.out.printf("Total WebSocket connections: %,d%n",
                totalConnections);
        System.out.printf("Total WebSocket reconnections: %,d%n",
                totalReconnections);

        double[] latencies = latencySamples.stream()
                .flatMapToDouble(Arrays::stream)
                .sorted()
                .toArray();

        if (latencies.length > 0) {
            double mean = Arrays.stream(latencies).average().orElse(0);

            System.out.printf(
                    "Push latency: mean=%.3f ms, median=%.3f ms, "
                            + "p95=%.3f ms, p99=%.3f ms%n",
                    mean,
                    percentile(latencies, 0.50),
                    percentile(latencies, 0.95),
                    percentile(latencies, 0.99));
        } else {
            System.out.println("No push-latency samples received.");
        }

        System.out.println(
                "Push latency includes server/client clock skew.");
    }

    private static double percentile(double[] sorted, double fraction) {
        int index = (int) Math.ceil(fraction * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }

    private static double secondsSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000_000.0;
    }
}