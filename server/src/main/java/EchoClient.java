import javax.websocket.ClientEndpoint;
import javax.websocket.ContainerProvider;
import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ClientEndpoint
public class EchoClient {

    private final CountDownLatch responsesRemaining;

    public EchoClient(CountDownLatch latch) {
        this.responsesRemaining = latch;
    }

    @OnMessage
    public void onMessage(String message) {
        System.out.println("Received: " + message);
        responsesRemaining.countDown();
    }

    public static void main(String[] args) throws Exception {
        int messages = 100;
        CountDownLatch latch = new CountDownLatch(messages);

        WebSocketContainer container =
                ContainerProvider.getWebSocketContainer();

        Session session = container.connectToServer(
                new EchoClient(latch),
                URI.create(
                        "ws://34.220.187.99:8080/ski-servlet/echo"
                )
        );

        try {
            long start = System.nanoTime();

            for (int i = 0; i < messages; i++) {
                session.getBasicRemote().sendText("msg-" + i);
            }

            boolean completed = latch.await(30, TimeUnit.SECONDS);

            double elapsedMs =
                    (System.nanoTime() - start) / 1_000_000.0;

            long received = messages - latch.getCount();

            System.out.println("Sent: " + messages);
            System.out.println("Received: " + received);
            System.out.printf("Elapsed: %.2f ms%n", elapsedMs);

            if (!completed) {
                throw new IllegalStateException(
                        "Timed out waiting for all echoes"
                );
            }

            System.out.println("SUCCESS: All 100 echoes received.");
        } finally {
            session.close();
        }
    }
}