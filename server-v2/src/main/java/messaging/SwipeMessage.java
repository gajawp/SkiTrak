package messaging;
import java.time.Instant;
public final class SwipeMessage {
    public final String eventId, resortID, dayID, skierID, liftID, timestamp, serverId;
    public final int vertical, time;
    public SwipeMessage(String id, String resort, String day, String skier,
                        int lift, int feet, int minute, String server) {
        eventId=id; resortID=resort; dayID=day; skierID=skier;
        liftID=Integer.toString(lift); vertical=feet; time=minute;
        timestamp=Instant.now().toString(); serverId=server;
    }
}
