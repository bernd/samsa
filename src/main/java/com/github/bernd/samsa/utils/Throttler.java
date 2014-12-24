package com.github.bernd.samsa.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 * <p/>
 * This class is thread-safe!
 */
public class Throttler {
    private static final Logger LOG = LoggerFactory.getLogger(Throttler.class);

    private final Object lock = new Object();
    private final double desiredRatePerSec;
    private final long checkIntervalMs;
    private final boolean throttleDown;
    private final Time time;

    private long periodStartNs;
    private double observedSoFar = 0.0;

    public Throttler(final double desiredRatePerSec) {
        this(desiredRatePerSec, 100L, true, new SystemTime(), "throttler", "entries");
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs) {
        this(desiredRatePerSec, checkIntervalMs, true, new SystemTime(), "throttler", "entries");
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown) {
        this(desiredRatePerSec, checkIntervalMs, throttleDown, new SystemTime(), "throttler", "entries");
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown,
                     final Time time) {
        this(desiredRatePerSec, checkIntervalMs, throttleDown, time, "throttler", "entries");
    }

    /**
     * @param desiredRatePerSec: The rate we want to hit in units/sec
     * @param checkIntervalMs:   The interval at which to check our rate
     * @param throttleDown:      Does throttling increase or decrease our rate?
     */
    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown,
                     final Time time,
                     final String metricName,
                     final String units) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalMs = checkIntervalMs;
        this.throttleDown = throttleDown;
        this.time = time;
        this.periodStartNs = time.nanoseconds();
    }

    public void maybeThrottle(final double observed) {
        //meter.mark(observed.toLong);
        synchronized (lock) {
            observedSoFar += observed;
            final long now = time.nanoseconds();
            final long elapsedNs = now - periodStartNs;
            // if we have completed an interval AND we have observed something, maybe
            // we should take a little nap
            if (elapsedNs > checkIntervalMs * Time.NS_PER_MS && observedSoFar > 0) {
                final double rateInSecs = (observedSoFar * Time.NS_PER_SEC) / elapsedNs;
                final boolean needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec));
                if (needAdjustment) {
                    // solve for the amount of time to sleep to make us hit the desired rate
                    final double desiredRateMs = desiredRatePerSec / (double) Time.MS_PER_SEC;
                    final long elapsedMs = elapsedNs / Time.NS_PER_MS;
                    final long sleepTime = Math.round((observedSoFar / desiredRateMs) - elapsedMs);
                    if (sleepTime > 0) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("Natural rate is %f per second but desired rate is %f, sleeping for %d ms to compensate.", rateInSecs, desiredRatePerSec, sleepTime));
                        }
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = now;
                observedSoFar = 0;
            }
        }
    }

    public static void main(final String[] args) throws InterruptedException {
        Random rand = new Random();
        Time time = new SystemTime();
        Throttler throttler = new Throttler(100000, 100, true, time);
        long interval = 30000;
        long start = time.milliseconds();
        long total = 0;
        while (true) {
            int value = rand.nextInt(1000);
            time.sleep(1);
            throttler.maybeThrottle(value);
            total += value;
            long now = time.milliseconds();
            if (now - start >= interval) {
                System.out.println(total / (interval / 1000.0));
                start = now;
                total = 0;
            }
        }
    }
}
