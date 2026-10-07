package benchmark;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static benchmark.DisruptorBenchmark.BUFFER_SIZE;
import static benchmark.DisruptorBenchmark.MESSAGE_COUNT;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 2)
@Fork(3)
@Threads(1)
public class DisruptorLatencyBenchmark {

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void publishAndAwaitConsumption(LatencyState state) {
        long expectedSequence = state.nextSequence++;
        state.ringBuffer.publishEvent(
                DisruptorBenchmark.FixEvent.TRANSLATOR,
                state.messages[(int) (expectedSequence & (MESSAGE_COUNT - 1))]
        );

        while (state.handler.processedSequence < expectedSequence) {
            Thread.onSpinWait();
        }
    }

    @State(Scope.Thread)
    public static class LatencyState {
        private final DisruptorBenchmark.FixMessage[] messages =
                DisruptorBenchmark.createFixTestData(MESSAGE_COUNT);
        private Disruptor<DisruptorBenchmark.FixEvent> disruptor;
        private com.lmax.disruptor.RingBuffer<DisruptorBenchmark.FixEvent> ringBuffer;
        private LatencyEventHandler handler;
        private long nextSequence;

        @Setup(Level.Trial)
        public void startDisruptor() {
            disruptor = new Disruptor<>(
                    DisruptorBenchmark.FixEvent.FACTORY,
                    BUFFER_SIZE,
                    runnable -> new Thread(runnable, "disruptor-latency-consumer"),
                    ProducerType.SINGLE,
                    new BusySpinWaitStrategy()
            );

            handler = new LatencyEventHandler();
            disruptor.handleEventsWith(handler);
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void stopDisruptor() {
            disruptor.shutdown();
            DisruptorBenchmark.consumeSink = handler.checksum;
        }
    }

    private static final class LatencyEventHandler implements EventHandler<DisruptorBenchmark.FixEvent> {
        private volatile long processedSequence = -1;
        private long checksum;

        @Override
        public void onEvent(DisruptorBenchmark.FixEvent event, long sequence, boolean endOfBatch) {
            checksum += event.sequenceNumber
                    + event.instrumentId
                    + event.orderId
                    + event.quantity
                    + event.priceInTicks
                    + event.sendingTimestamp
                    + event.side
                    + event.messageType;
            processedSequence = sequence;
        }
    }
}
