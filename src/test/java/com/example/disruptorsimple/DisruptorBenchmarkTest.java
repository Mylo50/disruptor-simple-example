package com.example.disruptorsimple;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisruptorBenchmarkTest {
    @Test
    void processesEveryPublishedEvent() {
        DisruptorBenchmark.BenchmarkResult result = DisruptorBenchmark.run(100_000);

        assertEquals(100_000, result.eventCount());
        assertEquals(100_000L * 99_999 / 2, result.checksum());
        assertTrue(result.elapsedNanos() > 0);
    }

    @Test
    void rejectsNonPositiveEventCounts() {
        assertThrows(IllegalArgumentException.class, () -> DisruptorBenchmark.run(0));
        assertThrows(IllegalArgumentException.class, () -> DisruptorBenchmark.run(-1));
    }
}
