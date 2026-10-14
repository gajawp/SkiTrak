import javax.websocket.ClientEndpoint;
import javax.websocket.ContainerProvider;
import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadEchoClient {

    @ClientEndpoint
    public static class Receiver {
        private final CountDownLatch echoes;

        public Receiver(CountDownLatch echoes) {
            this.echoes = echoes;
        }

        @OnMessage
        public void onMessage(String message) {
            echoes.countDown();
        }
    }

    public static void main(String[] args) throws Exception {
        int numThreads = 128;
        int messagesPerThread = 100;

        URI endpoint = URI.create(
                "ws://34.220.187.99:8080/ski-servlet/echo"
        );

        CountDownLatch threadsDone =
                new CountDownLatch(numThreads);

        AtomicInteger received = new AtomicInteger();
        AtomicInteger successfulConnections = new AtomicInteger();
        AtomicInteger failedWorkers = new AtomicInteger();

        long start = System.nanoTime();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;

            new Thread(() -> {
                Session session = null;
                CountDownLatch echoes =
                        new CountDownLatch(messagesPerThread);

                try {
                    WebSocketContainer container =
                            ContainerProvider.getWebSocketContainer();

                    session = container.connectToServer(
                            new Receiver(echoes), endpoint
                    );

                    successfulConnections.incrementAndGet();

                    for (int i = 0; i < messagesPerThread; i++) {
                        session.getBasicRemote().sendText(
                                "thread-" + threadId + "-msg-" + i
                        );
                    }

                    if (!echoes.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "Timed out waiting for echoes"
                        );
                    }
                } catch (Exception e) {
                    failedWorkers.incrementAndGet();
                    System.err.println(
                            "Thread " + threadId + ": " + e
                    );
                } finally {
                    if (session != null) {
                        try {
                            session.close();
                        } catch (Exception e) {
                            System.err.println(
                                    "Close error: " + e.getMessage()
                            );
                        }
                    }

                    received.addAndGet(
                            messagesPerThread - (int) echoes.getCount()
                    );
                    threadsDone.countDown();
                }
            }, "echo-worker-" + t).start();
        }

        threadsDone.await();

        double elapsedSeconds =
                (System.nanoTime() - start) / 1_000_000_000.0;

        int expected = numThreads * messagesPerThread;

        System.out.println("Threads: " + numThreads);
        System.out.println(
                "Connections opened: " + successfulConnections.get()
        );
        System.out.println("Expected echoes: " + expected);
        System.out.println("Received echoes: " + received.get());
        System.out.println("Failed workers: " + failedWorkers.get());

        System.out.printf(
                "Elapsed: %.2f ms%n", elapsedSeconds * 1000
        );
        System.out.printf(
                "Throughput: %.2f echoed messages/sec%n",
                received.get() / elapsedSeconds
        );

        if (failedWorkers.get() != 0 || received.get() != expected) {
            throw new IllegalStateException("Some workers failed");
        }

        System.out.println("SUCCESS: All echoes received.");
    }
}