import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Observes existing subscribers; does not submit swipes or stop servers. */
public final class WebSocketFailoverProbe {
    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: WebSocketFailoverProbe BASE_URL [durationSeconds]");
        }
        int seconds = args.length == 2 ? Integer.parseInt(args[1]) : 600;
        if (seconds < 1 || seconds > 3600) throw new IllegalArgumentException("Duration must be 1..3600 seconds");
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        List<LeaderboardSubscriber> subscribers = new ArrayList<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> subscribers.forEach(LeaderboardSubscriber::close)));
        try {
            for (int resort = 1; resort <= 10; resort++) {
                LeaderboardSubscriber subscriber = new LeaderboardSubscriber(client, args[0], resort);
                subscribers.add(subscriber);
            }
            for (LeaderboardSubscriber subscriber : subscribers) {
                // A failed initial attempt remains observable and retries through the existing subscriber.
                subscriber.start().exceptionally(error -> null);
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
            boolean readyPrinted = false;
            System.out.println("Counters sampled once per second; use timestamped close/error logs for outage timing.");
            System.out.println("Handshake duration is not total outage/recovery duration. Update counts include initial snapshots.");
            while (System.nanoTime() < deadline) {
                boolean ready = subscribers.stream().allMatch(s -> s.getConnections() > 0);
                if (ready && !readyPrinted) {
                    System.out.println(Instant.now() + " ALL_INITIAL_CONNECTIONS_ESTABLISHED");
                    readyPrinted = true;
                }
                for (LeaderboardSubscriber s : subscribers) {
                    double[] times = s.getConnectionTimesMs();
                    System.out.printf("%s resort=%d connections=%d reconnections=%d updates=%d invalid=%d failures=%d lastHandshakeMs=%.3f%n",
                            Instant.now(), s.getResortID(), s.getConnections(), s.getReconnections(),
                            s.getUpdates(), s.getInvalidMessages(), s.getConnectionFailures(),
                            times.length == 0 ? -1 : times[times.length - 1]);
                }
                Thread.sleep(1000);
            }
            System.out.println(Instant.now() + " RECORDING_COMPLETE: inspect reconnects and verify resumed live delivery separately.");
        } finally {
            subscribers.forEach(LeaderboardSubscriber::close);
        }
    }
}
