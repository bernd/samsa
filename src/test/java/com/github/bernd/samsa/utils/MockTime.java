package com.github.bernd.samsa.utils;

import java.util.concurrent.TimeUnit;

/**
 * A class used for unit testing things which depend on the Time interface.
 * <p/>
 * This class never manually advances the clock, it only does so when you call
 * sleep(ms)
 * <p/>
 * It also comes with an associated scheduler instance for managing background tasks in
 * a deterministic way.
 */
public class MockTime implements Time {
    public final MockScheduler scheduler;
    private volatile long currentMS;

    public MockTime() {
        this(System.currentTimeMillis());
    }

    public MockTime(final long currentMS) {
        this.currentMS = currentMS;
        this.scheduler = new MockScheduler(this);
    }

    @Override
    public long milliseconds() {
        return currentMS;
    }

    @Override
    public long nanoseconds() {
        return TimeUnit.NANOSECONDS.convert(currentMS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sleep(long ms) {
        currentMS += ms;
        scheduler.tick();
    }

    @Override
    public String toString() {
        return String.format("MockTime(%d)", milliseconds());
    }
}
