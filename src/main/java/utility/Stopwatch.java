package utility;

import java.time.Duration;
import java.time.Instant;

public class Stopwatch {
    private final Instant timestamp = Instant.now();

    public void report(String msg) {
        long time = Duration.between(timestamp, Instant.now()).toMillis();
        System.out.println(String.format("%s => %d ms", msg, time));
    }
}
