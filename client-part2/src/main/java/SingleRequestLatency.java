import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class SingleRequestLatency {

    public static void main(String[] args) throws Exception {
        String baseUrl = args.length > 0
                ? args[0]
                : "http://localhost:8080/ski-servlet";

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl
                        + "/skiers/1/seasons/2026/days/1/skiers/123"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"time\":217,\"liftID\":21}"))
                .build();

        // Measure the first request, including initial connection setup.
        double firstRequestMs = sendAndMeasure(client, request);

        // Warm the reused connection before collecting steady-state samples.
        for (int i = 0; i < 20; i++) {
            sendAndMeasure(client, request);
        }

        int samples = 100;
        double totalMs = 0;

        for (int i = 0; i < samples; i++) {
            totalMs += sendAndMeasure(client, request);
        }

        double meanMs = totalMs / samples;

        System.out.printf("Target: %s%n", baseUrl);
        System.out.printf("First request: %.3f ms%n", firstRequestMs);
        System.out.printf("Mean reused-connection RTT: %.3f ms%n", meanMs);
        System.out.printf("Measured requests: %d%n", samples);

        System.out.println("\nLittle's Law estimates:");
        for (int threads : new int[]{32, 64, 128, 256}) {
            double throughput = threads / (meanMs / 1000.0);
            System.out.printf(
                    "%d sender threads: %.2f swipes/second%n",
                    threads, throughput);
        }
    }

    private static double sendAndMeasure(
            HttpClient client, HttpRequest request) throws Exception {

        long start = System.nanoTime();

        HttpResponse<Void> response = client.send(
                request, HttpResponse.BodyHandlers.discarding());

        long elapsed = System.nanoTime() - start;

        if (response.statusCode() != 201) {
            throw new IllegalStateException(
                    "Expected HTTP 201, received "
                            + response.statusCode());
        }

        return elapsed / 1_000_000.0;
    }
}