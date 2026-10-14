import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class PerformanceMetrics {

    private final ConcurrentLinkedQueue<SwipeRecord> records =
            new ConcurrentLinkedQueue<>();

    public void record(long timestamp,
                       long responseTimestamp,
                       double latencyMs,
                       int statusCode,
                       int resortID) {

        records.add(new SwipeRecord(
                timestamp,
                responseTimestamp,
                latencyMs,
                statusCode,
                resortID));
    }

    public void writeCsv(Path outputFile) throws IOException {
        createParentDirectory(outputFile);

        try (BufferedWriter writer = Files.newBufferedWriter(
                outputFile, StandardCharsets.UTF_8)) {

            writer.write(
                    "timestamp,requestType,latency,statusCode,resortID,"
                            + "responseTimestamp");
            writer.newLine();

            for (SwipeRecord record : records) {
                writer.write(String.format(
                        Locale.US,
                        "%d,SWIPE,%.6f,%d,%d,%d",
                        record.timestamp,
                        record.latencyMs,
                        record.statusCode,
                        record.resortID,
                        record.responseTimestamp));
                writer.newLine();
            }
        }
    }

    public void printSummary(double ingestSeconds) {
        if (ingestSeconds <= 0) {
            throw new IllegalArgumentException(
                    "Ingest runtime must be positive");
        }

        double[] latencies = records.stream()
                .mapToDouble(record -> record.latencyMs)
                .sorted()
                .toArray();

        System.out.println("\n=== HTTP Attempt Latency ===");
        System.out.printf("Recorded attempts: %,d%n", latencies.length);
        System.out.println(
                "Includes retry attempts; excludes retry backoff.");
        System.out.println(
                "Status code 0 represents an attempt without an HTTP response.");

        if (latencies.length == 0) {
            return;
        }

        double mean = Arrays.stream(latencies).average().orElse(0);

        System.out.printf(Locale.US, "Mean: %.3f ms%n", mean);
        System.out.printf(Locale.US, "Median: %.3f ms%n",
                percentile(latencies, 0.50));
        System.out.printf(Locale.US, "p95: %.3f ms%n",
                percentile(latencies, 0.95));
        System.out.printf(Locale.US, "p99: %.3f ms%n",
                percentile(latencies, 0.99));
        System.out.printf(Locale.US, "Min: %.3f ms%n", latencies[0]);
        System.out.printf(Locale.US, "Max: %.3f ms%n",
                latencies[latencies.length - 1]);

        Map<Integer, Long> successfulByResort = new TreeMap<>();

        for (SwipeRecord record : records) {
            successfulByResort.putIfAbsent(record.resortID, 0L);

            if (record.statusCode == 201) {
                successfulByResort.merge(record.resortID, 1L, Long::sum);
            }
        }

        System.out.println("\n=== Throughput Per Resort ===");

        for (Map.Entry<Integer, Long> entry
                : successfulByResort.entrySet()) {

            System.out.printf(
                    Locale.US,
                    "Resort %d: %,d successful swipes, %.2f swipes/sec%n",
                    entry.getKey(),
                    entry.getValue(),
                    entry.getValue() / ingestSeconds);
        }
    }

    public void writeThroughputCsv(Path outputFile,
                                   long ingestStartMillis,
                                   double ingestSeconds)
            throws IOException {

        if (ingestSeconds <= 0) {
            throw new IllegalArgumentException(
                    "Ingest runtime must be positive");
        }

        createParentDirectory(outputFile);

        // Include empty buckets and normalize the final partial bucket
        // by its actual duration.
        int bucketCount = (int) Math.ceil(ingestSeconds / 10.0);
        long[] successfulCounts = new long[bucketCount];

        for (SwipeRecord record : records) {
            if (record.statusCode != 201) {
                continue;
            }

            long elapsedMillis =
                    record.responseTimestamp - ingestStartMillis;

            int bucket = (int) Math.floorDiv(elapsedMillis, 10_000L);

            // Wall-clock timestamps have millisecond rounding.
            bucket = Math.max(0, Math.min(bucket, bucketCount - 1));
            successfulCounts[bucket]++;
        }

        try (BufferedWriter writer = Files.newBufferedWriter(
                outputFile, StandardCharsets.UTF_8)) {

            writer.write(
                    "bucketStartSeconds,bucketEndSeconds,"
                            + "successfulSwipes,throughput");
            writer.newLine();

            for (int bucket = 0; bucket < bucketCount; bucket++) {
                double startSeconds = bucket * 10.0;
                double endSeconds = Math.min(
                        startSeconds + 10.0, ingestSeconds);

                double duration = endSeconds - startSeconds;
                double throughput = successfulCounts[bucket] / duration;

                writer.write(String.format(
                        Locale.US,
                        "%.3f,%.3f,%d,%.3f",
                        startSeconds,
                        endSeconds,
                        successfulCounts[bucket],
                        throughput));
                writer.newLine();
            }
        }
    }

    private static double percentile(double[] sorted, double fraction) {
        int index = (int) Math.ceil(fraction * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }

    private static void createParentDirectory(Path file)
            throws IOException {

        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static final class SwipeRecord {

        private final long timestamp;
        private final long responseTimestamp;
        private final double latencyMs;
        private final int statusCode;
        private final int resortID;

        private SwipeRecord(long timestamp,
                            long responseTimestamp,
                            double latencyMs,
                            int statusCode,
                            int resortID) {

            this.timestamp = timestamp;
            this.responseTimestamp = responseTimestamp;
            this.latencyMs = latencyMs;
            this.statusCode = statusCode;
            this.resortID = resortID;
        }
    }
}