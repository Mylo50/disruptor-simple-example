package com.example.disruptorsimple;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executors;

public final class DisruptorBenchmark {
    private static final int RING_BUFFER_SIZE = 1024;
    private static final long DEFAULT_EVENT_COUNT = 10_000_000;

    private DisruptorBenchmark() {
    }

    public static void main(String[] args) {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: DisruptorBenchmark [event-count]");
        }

        long eventCount = args.length == 0 ? DEFAULT_EVENT_COUNT : Long.parseLong(args[0]);
        BenchmarkResult result = run(eventCount);
        double eventsPerSecond = result.eventCount() * 1_000_000_000.0 / result.elapsedNanos();

        System.out.printf("Processed %,d events in %.3f s (%,.0f events/s); checksum=%d%n",
                result.eventCount(), result.elapsedNanos() / 1_000_000_000.0,
                eventsPerSecond, result.checksum());
    }

    public static BenchmarkResult run(long eventCount) {
        if (eventCount <= 0) {
            throw new IllegalArgumentException("event-count must be greater than zero");
        }

        LongEventHandler handler = new LongEventHandler();
        Disruptor<LongEvent> disruptor = new Disruptor<>(
                LongEvent::new,
                RING_BUFFER_SIZE,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new BlockingWaitStrategy());
        disruptor.handleEventsWith(handler);

        RingBuffer<LongEvent> ringBuffer = disruptor.start();
        long startNanos = System.nanoTime();
        for (long value = 0; value < eventCount; value++) {
            long sequence = ringBuffer.next();
            try {
                ringBuffer.get(sequence).setValue(value);
            } finally {
                ringBuffer.publish(sequence);
            }
        }
        disruptor.shutdown();
        long elapsedNanos = System.nanoTime() - startNanos;

        return new BenchmarkResult(eventCount, elapsedNanos, handler.getChecksum());
    }

    public record BenchmarkResult(long eventCount, long elapsedNanos, long checksum) {
    }

    private static final class LongEventHandler implements com.lmax.disruptor.EventHandler<LongEvent> {
        private long checksum;

        @Override
        public void onEvent(LongEvent event, long sequence, boolean endOfBatch) {
            checksum += event.getValue();
        }

        private long getChecksum() {
            return checksum;
        }
    }
}
