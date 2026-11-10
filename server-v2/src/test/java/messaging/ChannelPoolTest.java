package messaging;
import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class ChannelPoolTest {
    private static void awaitReady(ChannelPool p) throws Exception {
        long until=System.nanoTime()+5_000_000_000L;
        while(!p.ready() && System.nanoTime()<until) Thread.sleep(10);
        assertTrue(p.ready());
    }
    /** Mock channel that hands out sequence numbers and acks (or not) from inside basicPublish. */
    private static final class Fake {
        final Channel ch=mock(Channel.class);
        final AtomicLong seq=new AtomicLong(1);
        final ConfirmListener[] confirms=new ConfirmListener[1];
        final ReturnListener[] returns=new ReturnListener[1];
        Fake(boolean ack, boolean unroutable) throws Exception {
            when(ch.isOpen()).thenReturn(true);
            when(ch.getNextPublishSeqNo()).thenAnswer(i->seq.get());
            doAnswer(i->{confirms[0]=i.getArgument(0);return null;}).when(ch).addConfirmListener(any(ConfirmListener.class));
            doAnswer(i->{returns[0]=i.getArgument(0);return null;}).when(ch).addReturnListener(any(ReturnListener.class));
            doAnswer(i->{
                long tag=seq.getAndIncrement();
                AMQP.BasicProperties props=i.getArgument(3);
                if(unroutable) returns[0].handleReturn(312,"NO_ROUTE","x","k",props,new byte[0]);
                if(ack) confirms[0].handleAck(tag,false);
                return null;
            }).when(ch).basicPublish(anyString(),anyString(),eq(true),any(AMQP.BasicProperties.class),any(byte[].class));
        }
    }
    private static ConnectionFactory factory(Connection... cs) throws Exception {
        ConnectionFactory f=mock(ConnectionFactory.class);
        when(f.newConnection(anyString())).thenReturn(cs[0],java.util.Arrays.copyOfRange(cs,1,cs.length));
        for(Connection c:cs) when(c.isOpen()).thenReturn(true);
        return f;
    }
    @Test void reusesChannelAndWaitsForEveryConfirm() throws Exception {
        Connection c=mock(Connection.class); Fake a=new Fake(true,false);
        when(c.createChannel()).thenReturn(a.ch);
        try(ChannelPool p=new ChannelPool(factory(c),1)) {
            awaitReady(p);
            p.publish("one","resort.1",new byte[0]);
            p.publish("two","resort.1",new byte[0]);
            verify(c,times(1)).createChannel();
            verify(a.ch,times(2)).basicPublish(anyString(),anyString(),eq(true),any(AMQP.BasicProperties.class),any(byte[].class));
        }
        verify(c).abort();
    }
    @Test void concurrentPublishesShareOneChannel() throws Exception {
        Connection c=mock(Connection.class); Fake a=new Fake(true,false);
        when(c.createChannel()).thenReturn(a.ch);
        try(ChannelPool p=new ChannelPool(factory(c),1)) {
            awaitReady(p);
            Thread[] ts=new Thread[8]; AtomicBoolean failed=new AtomicBoolean();
            for(int i=0;i<ts.length;i++){final int n=i;
                ts[i]=new Thread(()->{try{for(int k=0;k<50;k++)p.publish("e"+n+"-"+k,"resort.1",new byte[0]);}
                catch(Exception e){failed.set(true);}});ts[i].start();}
            for(Thread t:ts)t.join();
            assertFalse(failed.get());
            verify(a.ch,times(400)).basicPublish(anyString(),anyString(),eq(true),any(AMQP.BasicProperties.class),any(byte[].class));
        }
    }
    @Test void unconfirmedPublishAbortsChannelAndBuildsNewConnection() throws Exception {
        Connection c1=mock(Connection.class),c2=mock(Connection.class);
        Fake a=new Fake(false,false),b=new Fake(true,false);
        when(c1.createChannel()).thenReturn(a.ch);when(c2.createChannel()).thenReturn(b.ch);
        AtomicBoolean open=new AtomicBoolean(true);
        when(a.ch.isOpen()).thenAnswer(i -> open.get());
        doAnswer(i -> {open.set(false);return null;}).when(a.ch).abort();
        try(ChannelPool p=new ChannelPool(factory(c1,c2),1)) {
            awaitReady(p);
            assertThrows(IOException.class,() -> p.publish("one","resort.1",new byte[0]));
            awaitReady(p);
            p.publish("one","resort.1",new byte[0]);
            verify(b.ch).basicPublish(anyString(),anyString(),eq(true),any(AMQP.BasicProperties.class),any(byte[].class));
        }
    }
    @Test void nackFailsPublish() throws Exception {
        Connection c=mock(Connection.class); Fake a=new Fake(false,false);
        when(c.createChannel()).thenReturn(a.ch);
        doAnswer(i->{a.confirms[0].handleNack(a.seq.getAndIncrement(),false);return null;})
                .when(a.ch).basicPublish(anyString(),anyString(),eq(true),any(AMQP.BasicProperties.class),any(byte[].class));
        try(ChannelPool p=new ChannelPool(factory(c),1)) {
            awaitReady(p);
            assertThrows(IOException.class,()->p.publish("id","resort.1",new byte[0]));
        }
    }
    @Test void mandatoryReturnFailsEvenWhenBrokerConfirms() throws Exception {
        Connection c=mock(Connection.class); Fake a=new Fake(true,true);
        when(c.createChannel()).thenReturn(a.ch);
        try(ChannelPool p=new ChannelPool(factory(c),1)) {
            awaitReady(p);
            assertThrows(IOException.class,()->p.publish("id","resort.1",new byte[0]));
        }
    }
}
