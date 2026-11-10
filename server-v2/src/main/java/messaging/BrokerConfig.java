package messaging;
import com.rabbitmq.client.ConnectionFactory;
public final class BrokerConfig {
    private BrokerConfig() {}
    public static String env(String key, String fallback) {
        String value = System.getenv(key); return value == null ? fallback : value;
    }
    public static int positive(String key, int fallback) {
        int value = Integer.parseInt(env(key, Integer.toString(fallback)));
        if (value <= 0) throw new IllegalArgumentException(key + " must be positive");
        return value;
    }
    public static ConnectionFactory factory() {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(env("RABBITMQ_HOST", "localhost"));
        f.setPort(positive("RABBITMQ_PORT", 5672));
        f.setUsername(env("RABBITMQ_USER", "skitrak"));
        f.setPassword(env("RABBITMQ_PASSWORD", "change-me"));
        f.setVirtualHost(env("RABBITMQ_VHOST", "/"));
        f.setConnectionTimeout(3000); f.setHandshakeTimeout(3000);
        f.setRequestedHeartbeat(15);
        // Recovery belongs to ChannelPool: never mix two reconnect owners.
        f.setAutomaticRecoveryEnabled(false);
        return f;
    }
}
