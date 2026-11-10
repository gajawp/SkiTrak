import messaging.*;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import com.google.gson.*;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class SkierServletTest {
    @Test void validSwipePublishesProvidedVerticalAndReturns201() throws Exception {
        SwipePublisher pool=mock(SwipePublisher.class);
        ServletContext context=mock(ServletContext.class);
        ServletConfig config=mock(ServletConfig.class);
        when(config.getServletContext()).thenReturn(context);
        when(context.getAttribute(AppLifecycle.POOL)).thenReturn(pool);
        SkierServlet servlet=new SkierServlet();servlet.init(config);
        HttpServletRequest req=mock(HttpServletRequest.class);
        HttpServletResponse res=mock(HttpServletResponse.class);
        StringWriter output=new StringWriter();when(res.getWriter()).thenReturn(new PrintWriter(output));
        when(req.getPathInfo()).thenReturn("/1/seasons/2026/days/1/skiers/123");
        when(req.getReader()).thenReturn(new BufferedReader(new StringReader("{\"time\":217,\"liftID\":1}")));
        String id="00000000-0000-4000-8000-000000000001";
        when(req.getHeader("X-Event-ID")).thenReturn(id);
        servlet.doPost(req,res);
        ArgumentCaptor<byte[]> bytes=ArgumentCaptor.forClass(byte[].class);
        verify(pool).publish(eq(id),eq("resort.1"),bytes.capture());verify(res).setStatus(201);
        JsonObject event=JsonParser.parseString(new String(bytes.getValue(),StandardCharsets.UTF_8)).getAsJsonObject();
        assertEquals(167,event.get("vertical").getAsInt());assertEquals("123",event.get("skierID").getAsString());
    }
    @Test void unconfirmedPublishNeverReturns201() throws Exception {
        SwipePublisher pool=mock(SwipePublisher.class);
        doThrow(new IOException("broker down")).when(pool).publish(anyString(),anyString(),any(byte[].class));
        ServletContext context=mock(ServletContext.class);ServletConfig config=mock(ServletConfig.class);
        when(config.getServletContext()).thenReturn(context);when(context.getAttribute(AppLifecycle.POOL)).thenReturn(pool);
        SkierServlet servlet=new SkierServlet();servlet.init(config);
        HttpServletRequest req=mock(HttpServletRequest.class);HttpServletResponse res=mock(HttpServletResponse.class);
        when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
        when(req.getPathInfo()).thenReturn("/1/seasons/2026/days/1/skiers/123");
        when(req.getReader()).thenReturn(new BufferedReader(new StringReader("{\"time\":217,\"liftID\":1}")));
        servlet.doPost(req,res);verify(res).setStatus(503);verify(res,never()).setStatus(201);
    }
}
