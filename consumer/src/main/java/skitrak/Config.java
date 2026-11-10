package skitrak;
import com.rabbitmq.client.ConnectionFactory;
public final class Config {
    private Config() {}
    public static String env(String key,String fallback) {String v=System.getenv(key);return v==null?fallback:v;}
    public static int number(String key,int fallback) {
        int n=Integer.parseInt(env(key,Integer.toString(fallback)));
        if(n<1)throw new IllegalArgumentException(key+" must be positive");return n;
    }
    public static ConnectionFactory factory() {
        ConnectionFactory f=new ConnectionFactory();
        f.setHost(env("RABBITMQ_HOST","localhost"));f.setPort(number("RABBITMQ_PORT",5672));
        f.setUsername(env("RABBITMQ_USER","skitrak"));f.setPassword(env("RABBITMQ_PASSWORD","change-me"));
        f.setVirtualHost(env("RABBITMQ_VHOST","/"));f.setConnectionTimeout(3000);
        f.setHandshakeTimeout(3000);f.setRequestedHeartbeat(15);f.setAutomaticRecoveryEnabled(false);
        return f;
    }
}
