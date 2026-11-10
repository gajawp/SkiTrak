package leaderboard;
import org.junit.jupiter.api.Test;
import javax.websocket.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class LiveHubTest {
    private String snapshot(String resort,String day,long version,int feet) {
        return "{\"resortID\":\""+resort+"\",\"dayID\":\""+day+"\",\"version\":"+version+
            ",\"serverTimestamp\":\"2026-11-01T23:15:39Z\",\"top\":[{\"rank\":1,\"skierID\":\"123\",\"vertical\":"+feet+"}]}";
    }
    static class Client {
        final Session session=mock(Session.class);
        final RemoteEndpoint.Async async=mock(RemoteEndpoint.Async.class);
        final List<String> messages=new ArrayList<>();
        final List<SendHandler> completions=new ArrayList<>();
        Client() {
            when(session.isOpen()).thenReturn(true);when(session.getAsyncRemote()).thenReturn(async);
            doAnswer(i->{messages.add(i.getArgument(0));completions.add(i.getArgument(1));return null;})
                .when(async).sendText(anyString(),any(SendHandler.class));
        }
        void complete(int i){completions.get(i).onResult(new SendResult());}
    }
    @Test void routesByResortAndDayAndIgnoresOlderVersions() throws Exception {
        try(LiveHub hub=new LiveHub("http://127.0.0.1:1")) {
            Client a=new Client(),b=new Client(),c=new Client();
            hub.add(a.session,"1","1");hub.add(b.session,"2","1");hub.add(c.session,"1","2");
            hub.accept(snapshot("1","1",2,241));a.complete(0);
            hub.accept(snapshot("1","1",1,167));hub.accept(snapshot("1","1",2,241));
            assertEquals(1,a.messages.size());assertTrue(b.messages.isEmpty());assertTrue(c.messages.isEmpty());
            assertTrue(a.messages.get(0).contains("\"vertical\":241"));
            assertTrue(a.messages.get(0).contains("aggregationTimestamp"));
        }
    }
    @Test void slowClientCoalescesUpdatesWithoutOverlappingSends() throws Exception {
        try(LiveHub hub=new LiveHub("http://127.0.0.1:1")) {
            Client c=new Client();hub.add(c.session,"1","1");
            hub.accept(snapshot("1","1",1,167));
            hub.accept(snapshot("1","1",2,241));hub.accept(snapshot("1","1",3,585));
            assertEquals(1,c.messages.size());c.complete(0);
            assertEquals(2,c.messages.size());assertTrue(c.messages.get(1).contains("\"version\":3"));
            c.complete(1);assertEquals(2,c.messages.size());
        }
    }
    @Test void newSubscriberGetsCachedSnapshotAndFailureClosesIt() throws Exception {
        try(LiveHub hub=new LiveHub("http://127.0.0.1:1")) {
            hub.accept(snapshot("1","1",2,241));Client c=new Client();hub.add(c.session,"1","1");
            assertEquals(1,c.messages.size());
            c.completions.get(0).onResult(new SendResult(new java.io.IOException("closed")));
            verify(c.session).close();hub.accept(snapshot("1","1",3,585));assertEquals(1,c.messages.size());
        }
    }
    @Test void malformedSnapshotCannotReplaceGoodCache() {
        try(LiveHub hub=new LiveHub("http://127.0.0.1:1")) {
            hub.accept(snapshot("1","1",2,241));
            assertThrows(RuntimeException.class,()->hub.accept("{}"));
            Client c=new Client();hub.add(c.session,"1","1");assertTrue(c.messages.get(0).contains("\"version\":2"));
        }
    }
}
