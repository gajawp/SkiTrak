import leaderboard.*;
import javax.websocket.*;
import javax.websocket.server.*;
import messaging.BrokerConfig;
import java.io.IOException;
import java.util.List;
@ServerEndpoint("/leaderboard/{resortID}")
public final class LeaderboardEndpoint {
    @OnOpen public void open(Session session,@PathParam("resortID") String resort) throws IOException {
        List<String> days=session.getRequestParameterMap().get("dayID");
        String day=days==null?BrokerConfig.env("LEADERBOARD_DAY_ID","1"):
            (days.size()==1?days.get(0):"");
        if(!Snapshot.positive(resort)||Integer.parseInt(resort)>10||!Snapshot.positive(day)) {
            session.close(new CloseReason(CloseReason.CloseCodes.CANNOT_ACCEPT,"Invalid resortID or dayID"));return;
        }
        LiveHub hub=LiveHub.current;
        if(hub==null){session.close(new CloseReason(CloseReason.CloseCodes.TRY_AGAIN_LATER,"Server starting"));return;}
        hub.add(session,resort,day);
    }
    @OnClose public void close(Session session){LiveHub hub=LiveHub.current;if(hub!=null && session!=null)hub.remove(session);}
    @OnError public void error(Session session,Throwable error) throws IOException {
        close(session);if(session!=null&&session.isOpen())session.close();
    }
}
