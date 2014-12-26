package com.github.bernd.samsa.utils;

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time
 */
public interface Time {
    public static final long NS_PER_US = 1000;
    public static final long US_PER_MS = 1000;
    public static final long MS_PER_SEC = 1000;
    public static final long NS_PER_MS = NS_PER_US * US_PER_MS;
    public static final long NS_PER_SEC = NS_PER_MS * MS_PER_SEC;
    public static final long US_PER_SEC = US_PER_MS * MS_PER_SEC;
    public static final long SECS_PER_MIN = 60;
    public static final long MINS_PER_HOUR = 60;
    public static final long HOURS_PER_DAY = 24;
    public static final long SECS_PER_HOUR = SECS_PER_MIN * MINS_PER_HOUR;
    public static final long SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;
    public static final long MINS_PER_DAY = MINS_PER_HOUR * HOURS_PER_DAY;

    /**
     * The current time in milliseconds
     */
    long milliseconds();

    /**
     * The current time in nanoseconds
     */
    long nanoseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    void sleep(long ms);
}
