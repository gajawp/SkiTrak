import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;

@ServerEndpoint("/echo")
public class EchoEndpoint {

    @OnOpen
    public void onOpen(Session session) {
        System.out.println("Opened: " + session.getId());
    }

    @OnMessage
    public void onMessage(String message, Session session)
            throws IOException {

        session.getBasicRemote().sendText(
                "echo@" + System.currentTimeMillis() + ": " + message
        );
    }

    @OnClose
    public void onClose(Session session) {
        System.out.println("Closed: " + session.getId());
    }

    @OnError
    public void onError(Session session, Throwable error) {
        String sessionId = session == null
                ? "unknown"
                : session.getId();

        System.err.println(
                "Error on " + sessionId + ": " + error.getMessage()
        );
    }
}