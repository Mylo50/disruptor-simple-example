# Disruptor simple example

A small Java example of the LMAX Disruptor: a producer publishes numbered events
to a ring buffer, and a consumer handles them in sequence. The benchmark reports
the time and throughput for publishing and processing a chosen number of events.

## Requirements

- JDK 17 or newer
- Maven 3.8 or newer

## Run

Run the tests and build:

```sh
mvn test
```

Run the benchmark with the default of 10 million events:

```sh
mvn compile exec:java
```

Or pass an event count:

```sh
mvn compile exec:java -Dexec.args="1000000"
```

This is an introductory throughput example, not a substitute for a dedicated
benchmark harness. Results depend on the JVM, hardware, and other running work.
