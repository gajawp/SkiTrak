package messaging;
import java.io.IOException;
public interface SwipePublisher {
    void publish(String eventId, String routingKey, byte[] body) throws IOException, InterruptedException;
}
