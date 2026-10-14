import com.google.gson.Gson;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ServerEndpoint("/leaderboard/{resortID}")
public class LeaderboardEndpoint {

    private static final Gson GSON = new Gson();

    // Each resort has its own set of subscribers.
    private static final Map<String, Set<Session>> SUBSCRIBERS =
            new ConcurrentHashMap<>();

    // Prevent overlapping asynchronous sends to the same subscriber.
    private static final Set<Session> PENDING_SENDS =
            ConcurrentHashMap.newKeySet();

    private static final ScheduledExecutorService PUSHER =
            Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "leaderboard-pusher");
                thread.setDaemon(true);
                return thread;
            });

    static {
        PUSHER.scheduleAtFixedRate(
                LeaderboardEndpoint::pushAll,
                500,
                500,
                TimeUnit.MILLISECONDS);
    }

    @OnOpen
    public void onOpen(Session session,
                       @PathParam("resortID") String resortID)
            throws IOException {

        if (!isPositiveInteger(resortID)) {
            session.close(new javax.websocket.CloseReason(
                    javax.websocket.CloseReason.CloseCodes.CANNOT_ACCEPT,
                    "resortID must be a positive integer"));
            return;
        }

        session.getAsyncRemote().setSendTimeout(2000);

        SUBSCRIBERS.compute(resortID, (key, sessions) -> {
            if (sessions == null) {
                sessions = ConcurrentHashMap.newKeySet();
            }
            sessions.add(session);
            return sessions;
        });
    }

    @OnClose
    public void onClose(Session session,
                        @PathParam("resortID") String resortID) {
        removeSubscriber(session, resortID);
    }

    @OnError
    public void onError(Session session,
                        Throwable error,
                        @PathParam("resortID") String resortID) {
        if (session != null) {
            removeSubscriber(session, resortID);
            closeQuietly(session);
        }
    }

    private static void pushAll() {
        for (Map.Entry<String, Set<Session>> entry
                : SUBSCRIBERS.entrySet()) {

            String resortID = entry.getKey();

            for (Session session : entry.getValue()) {
                if (!session.isOpen()) {
                    removeSubscriber(session, resortID);
                    continue;
                }

                if (!PENDING_SENDS.add(session)) {
                    continue;
                }

                try {
                    // Timestamp is generated immediately before sending.
                    String json = syntheticLeaderboard(resortID);

                    session.getAsyncRemote().sendText(json, result -> {
                        PENDING_SENDS.remove(session);

                        if (!result.isOK()) {
                            removeSubscriber(session, resortID);
                            closeQuietly(session);
                        }
                    });
                } catch (RuntimeException error) {
                    PENDING_SENDS.remove(session);
                    removeSubscriber(session, resortID);
                    closeQuietly(session);
                }
            }
        }
    }

    private static String syntheticLeaderboard(String resortID) {
        List<Map<String, Object>> top = new ArrayList<>();

        // Canned standings: deliberately unrelated to received swipes.
        top.add(skierEntry(1, 40213, 44120));
        top.add(skierEntry(2, 11872, 43880));
        top.add(skierEntry(3, 76501, 42600));
        top.add(skierEntry(4, 23904, 41200));
        top.add(skierEntry(5, 58132, 39800));

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("resortID", resortID);
        message.put("serverTimestamp", Instant.now().toString());
        message.put("top", top);

        return GSON.toJson(message);
    }

    private static Map<String, Object> skierEntry(
            int rank, int skierID, int vertical) {

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("rank", rank);
        entry.put("skierID", skierID);
        entry.put("vertical", vertical);
        return entry;
    }

    private static void removeSubscriber(
            Session session, String resortID) {

        PENDING_SENDS.remove(session);

        SUBSCRIBERS.computeIfPresent(resortID, (key, sessions) -> {
            sessions.remove(session);
            return sessions.isEmpty() ? null : sessions;
        });
    }

    private static boolean isPositiveInteger(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character < '0' || character > '9') {
                return false;
            }
        }

        try {
            return Integer.parseInt(value) > 0;
        } catch (NumberFormatException error) {
            return false;
        }
    }

    private static void closeQuietly(Session session) {
        try {
            if (session.isOpen()) {
                session.close();
            }
        } catch (IOException ignored) {
            // Subscriber has already been removed.
        }
    }

    // Stop the timer when Tomcat stops or undeploys this application.
    @WebListener
    public static class ShutdownListener
            implements ServletContextListener {

        @Override
        public void contextDestroyed(ServletContextEvent event) {
            PUSHER.shutdownNow();

            for (Set<Session> sessions : SUBSCRIBERS.values()) {
                for (Session session : sessions) {
                    closeQuietly(session);
                }
            }

            SUBSCRIBERS.clear();
            PENDING_SENDS.clear();
        }
    }
}