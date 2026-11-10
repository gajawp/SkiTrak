package messaging;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.BuiltinExchangeType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public final class Topology {
    public static final String EXCHANGE = "skitrak.exchange";
    private Topology() {}
    public static void declare(Channel c) throws IOException {
        c.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
        c.exchangeDeclare("skitrak.dead", BuiltinExchangeType.TOPIC, true);
        c.queueDeclare("skitrak.dead", true, false, false, null);
        c.queueBind("skitrak.dead", "skitrak.dead", "#");
        Map<String,Object> args = new HashMap<>();
        args.put("x-message-ttl", BrokerConfig.positive("QUEUE_TTL_MS", 86400000));
        args.put("x-max-length", BrokerConfig.positive("QUEUE_MAX_LENGTH", 1000000));
        args.put("x-overflow", "reject-publish");
        args.put("x-dead-letter-exchange", "skitrak.dead");
        for (int r = 1; r <= 10; r++) {
            String name = "resort." + r;
            c.queueDeclare(name, true, false, false, args);
            c.queueBind(name, EXCHANGE, name);
        }
    }
}
