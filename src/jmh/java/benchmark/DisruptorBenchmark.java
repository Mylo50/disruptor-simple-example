package benchmark;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 2)
@Fork(3)
@Threads(1)
public class DisruptorBenchmark {

    static final int BUFFER_SIZE = 16_384;
    static final int MESSAGE_COUNT = 1_024;
    private static final int BATCH_SIZE = 256;
    static volatile long consumeSink;

    private final FixMessage[] messages = createFixTestData(MESSAGE_COUNT);
    private int nextMessageIndex;
    private Disruptor<FixEvent> disruptor;
    private com.lmax.disruptor.RingBuffer<FixEvent> ringBuffer;
    private FixEventHandler handler;

    @Setup(Level.Trial)
    public void startDisruptor() {
        disruptor = new Disruptor<>(
                FixEvent.FACTORY,
                BUFFER_SIZE,
                runnable -> new Thread(runnable, "disruptor-consumer"),
                ProducerType.SINGLE,
                new BusySpinWaitStrategy()
        );

        handler = new FixEventHandler();
        disruptor.handleEventsWith(handler);
        ringBuffer = disruptor.start();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void publishBatch() {
        int messageIndex = nextMessageIndex;
        for (int i = 0; i < BATCH_SIZE; i++) {
            ringBuffer.publishEvent(FixEvent.TRANSLATOR, messages[messageIndex]);
            messageIndex = (messageIndex + 1) & (MESSAGE_COUNT - 1);
        }
        nextMessageIndex = messageIndex;
    }

    @TearDown(Level.Trial)
    public void stopDisruptor() {
        disruptor.shutdown();
        consumeSink = handler.checksum;
    }

    static FixMessage[] createFixTestData(int count) {
        FixMessage[] data = new FixMessage[count];
        long baseTimestamp = System.nanoTime();

        for (int i = 0; i < count; i++) {
            data[i] = new FixMessage(
                    i + 1L,
                    (i % 2_048) + 1L,
                    10_000_000L + i,
                    (i % 200) + 1,
                    100_000 + (i % 1_000),
                    baseTimestamp + i,
                    (byte) (((i & 1) == 0) ? '1' : '2'),
                    (byte) 'D'
            );
        }
        return data;
    }

    record FixMessage(
            long sequenceNumber,
            long instrumentId,
            long orderId,
            int quantity,
            int priceInTicks,
            long sendingTimestamp,
            byte side,
            byte messageType
    ) {
    }

    static final class FixEvent {
        long sequenceNumber;
        long instrumentId;
        long orderId;
        int quantity;
        int priceInTicks;
        long sendingTimestamp;
        byte side;
        byte messageType;

        static final EventFactory<FixEvent> FACTORY = FixEvent::new;

        static final EventTranslatorOneArg<FixEvent, FixMessage> TRANSLATOR =
                (event, sequence, message) -> {
                    event.sequenceNumber = message.sequenceNumber();
                    event.instrumentId = message.instrumentId();
                    event.orderId = message.orderId();
                    event.quantity = message.quantity();
                    event.priceInTicks = message.priceInTicks();
                    event.sendingTimestamp = message.sendingTimestamp();
                    event.side = message.side();
                    event.messageType = message.messageType();
                };
    }

    static final class FixEventHandler implements EventHandler<FixEvent> {
        long checksum;

        @Override
        public void onEvent(FixEvent event, long sequence, boolean endOfBatch) {
            checksum += event.sequenceNumber
                    + event.instrumentId
                    + event.orderId
                    + event.quantity
                    + event.priceInTicks
                    + event.sendingTimestamp
                    + event.side
                    + event.messageType;
        }
    }

}
