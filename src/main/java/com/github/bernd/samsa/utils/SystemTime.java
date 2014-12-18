package com.github.bernd.samsa.utils;

import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.TimeUnit;

/**
 * The normal system implementation of time functions
 */
public class SystemTime implements SamsaTime {
    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }

    @Override
    public void sleepUninterruptibly(long ms) {
        Uninterruptibles.sleepUninterruptibly(ms, TimeUnit.MILLISECONDS);
    }
}
