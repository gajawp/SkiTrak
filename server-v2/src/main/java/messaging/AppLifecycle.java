package messaging;
import leaderboard.*;
import javax.servlet.*;
import javax.servlet.annotation.WebListener;
@WebListener
public final class AppLifecycle implements ServletContextListener {
    public static final String POOL="skitrak.channelPool",BACKPLANE="skitrak.backplane";
    private LiveHub hub;
    @Override public void contextInitialized(ServletContextEvent e) {
        hub=new LiveHub(BrokerConfig.env("CONSUMER_BASE_URL","http://localhost:9090"));
        Backplane backplane=new Backplane(hub,BrokerConfig.env("SERVER_ID","ingest-local"));
        e.getServletContext().setAttribute(POOL,new ChannelPool(BrokerConfig.factory(),BrokerConfig.positive("CHANNEL_POOL_SIZE",32)));
        e.getServletContext().setAttribute(BACKPLANE,backplane);
        LiveHub.current=hub;hub.start();backplane.start();
    }
    @Override public void contextDestroyed(ServletContextEvent e) {
        Backplane b=(Backplane)e.getServletContext().getAttribute(BACKPLANE);if(b!=null)b.close();
        LiveHub.current=null;if(hub!=null)hub.close();
        ChannelPool p=(ChannelPool)e.getServletContext().getAttribute(POOL);if(p!=null)p.close();
    }
}
