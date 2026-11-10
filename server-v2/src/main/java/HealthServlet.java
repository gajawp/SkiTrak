import messaging.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.IOException;
@WebServlet("/health")
public final class HealthServlet extends HttpServlet {
    @Override protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        ChannelPool pool=(ChannelPool)getServletContext().getAttribute(AppLifecycle.POOL);
        leaderboard.Backplane backplane=(leaderboard.Backplane)getServletContext().getAttribute(AppLifecycle.BACKPLANE);
        boolean ready=pool!=null && pool.ready() && backplane!=null && backplane.ready();
        res.setStatus(ready ? 200 : 503); res.setContentType("application/json");
        res.getWriter().write(ready ? "{\"status\":\"UP\"}" : "{\"status\":\"DOWN\"}");
    }
}
