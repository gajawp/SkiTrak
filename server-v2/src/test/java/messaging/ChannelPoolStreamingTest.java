package messaging;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ChannelPoolStreamingTest {
    static final class Fixture implements AutoCloseable {
        final Channel channel = mock(Channel.class);
        final Connection connection = mock(Connection.class);
        final ConnectionFactory factory = mock(ConnectionFactory.class);
        final ConfirmListener[] confirm = new ConfirmListener[1];
        final ReturnListener[] returned = new ReturnListener[1];
        final ShutdownListener[] shutdown = new ShutdownListener[1];
        final AtomicLong sequence = new AtomicLong(1);
        final List<AMQP.BasicProperties> sent = new CopyOnWriteArrayList<>();
        final ExecutorService callers = Executors.newFixedThreadPool(4);
        final ChannelPool pool;
        Fixture(int capacity, long admission, long timeout) throws Exception {
            when(channel.isOpen()).thenReturn(true);
            when(connection.isOpen()).thenReturn(true);
            when(factory.newConnection(anyString())).thenReturn(connection);
            when(connection.createChannel()).thenReturn(channel);
            when(channel.getNextPublishSeqNo()).thenAnswer(i -> sequence.get());
            doAnswer(i -> { confirm[0] = i.getArgument(0); return null; })
                    .when(channel).addConfirmListener(any(ConfirmListener.class));
            doAnswer(i -> { returned[0] = i.getArgument(0); return null; })
                    .when(channel).addReturnListener(any(ReturnListener.class));
            doAnswer(i -> { shutdown[0] = i.getArgument(0); return null; })
                    .when(channel).addShutdownListener(any(ShutdownListener.class));
            doAnswer(i -> { sequence.incrementAndGet(); sent.add(i.getArgument(3)); return null; })
                    .when(channel).basicPublish(anyString(), anyString(), eq(true),
                            any(AMQP.BasicProperties.class), any(byte[].class));
            pool = new ChannelPool(factory, 1, capacity, admission, timeout);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!pool.ready() && System.nanoTime() < end) Thread.sleep(5);
            assertTrue(pool.ready());
        }
        Future<Void> send(String id) {
            return callers.submit(() -> { pool.publish(id, "resort.1", new byte[0]); return null; });
        }
        void awaitSent(int count) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (sent.size() < count && System.nanoTime() < end) Thread.sleep(2);
            assertEquals(count, sent.size());
        }
        public void close() { pool.close(); callers.shutdownNow(); }
    }

    @Test void overlappingDuplicateIdsHaveSeparateReturnCorrelation() throws Exception {
        try (Fixture f = new Fixture(8, 100, 2000)) {
            Future<Void> a = f.send("same-event"); f.awaitSent(1);
            Future<Void> b = f.send("same-event"); f.awaitSent(2);
            assertFalse(a.isDone()); assertFalse(b.isDone());
            assertEquals("same-event", f.sent.get(0).getMessageId());
            assertEquals("same-event", f.sent.get(1).getMessageId());
            assertNotEquals(f.sent.get(0).getCorrelationId(), f.sent.get(1).getCorrelationId());
            assertEquals(2, f.sent.get(0).getDeliveryMode().intValue());
            f.returned[0].handleReturn(312, "NO_ROUTE", "x", "k", f.sent.get(0), new byte[0]);
            f.confirm[0].handleAck(2, true);
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> a.get(1, TimeUnit.SECONDS));
            assertTrue(error.getCause().getMessage().contains("Unroutable"));
            assertNull(b.get(1, TimeUnit.SECONDS));
        }
    }

    @Test void cumulativeAckDoesNotCompleteLaterSequence() throws Exception {
        try (Fixture f = new Fixture(8, 100, 2000)) {
            Future<Void> a = f.send("a"); f.awaitSent(1);
            Future<Void> b = f.send("b"); f.awaitSent(2);
            Future<Void> c = f.send("c"); f.awaitSent(3);
            f.confirm[0].handleAck(2, true);
            a.get(1, TimeUnit.SECONDS); b.get(1, TimeUnit.SECONDS);
            assertFalse(c.isDone());
            f.confirm[0].handleAck(3, false); c.get(1, TimeUnit.SECONDS);
        }
    }

    @Test void capacityIsBoundedAndReleasedAfterConfirm() throws Exception {
        try (Fixture f = new Fixture(1, 50, 2000)) {
            Future<Void> a = f.send("a"); f.awaitSent(1);
            Future<Void> b = f.send("b");
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> b.get(1, TimeUnit.SECONDS));
            assertTrue(error.getCause().getMessage().contains("capacity busy"));
            assertEquals(1, f.sent.size());
            f.confirm[0].handleAck(1, false); a.get(1, TimeUnit.SECONDS);
            Future<Void> c = f.send("c"); f.awaitSent(2);
            f.confirm[0].handleAck(2, false); c.get(1, TimeUnit.SECONDS);
        }
    }

    @Test void channelShutdownFailsOutstandingAndLateAckCannotRescueIt() throws Exception {
        try (Fixture f = new Fixture(8, 100, 2000)) {
            Future<Void> a = f.send("a"); f.awaitSent(1);
            f.shutdown[0].shutdownCompleted(new ShutdownSignalException(false, false, null, f.channel));
            assertThrows(ExecutionException.class, () -> a.get(1, TimeUnit.SECONDS));
            f.confirm[0].handleAck(1, false);
            assertFalse(f.pool.ready());
        }
    }

    @Test void callerDeadlineIncludesBlockedSendingThread() throws Exception {
        CountDownLatch unblock = new CountDownLatch(1);
        try (Fixture f = new Fixture(8, 100, 150)) {
            doAnswer(i -> {
                while (true) {
                    try { unblock.await(); break; } catch (InterruptedException ignored) { }
                }
                return null;
            }).when(f.channel).basicPublish(anyString(), anyString(), eq(true),
                    any(AMQP.BasicProperties.class), any(byte[].class));
            Future<Void> a = f.send("a");
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> a.get(1, TimeUnit.SECONDS));
            assertTrue(error.getCause() instanceof IOException);
            assertFalse(f.pool.ready());
        } finally { unblock.countDown(); }
    }

    @Test void closeFailsOutstandingAndRejectsFurtherPublishes() throws Exception {
        try (Fixture f = new Fixture(8, 100, 2000)) {
            Future<Void> a = f.send("a"); f.awaitSent(1);
            f.pool.close();
            assertThrows(ExecutionException.class, () -> a.get(1, TimeUnit.SECONDS));
            assertThrows(IOException.class, () -> f.pool.publish("b", "resort.1", new byte[0]));
        }
    }
}
