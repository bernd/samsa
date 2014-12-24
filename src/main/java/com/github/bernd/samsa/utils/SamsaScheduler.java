package com.github.bernd.samsa.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * <p/>
 * It has a pool of samsa-scheduler- threads that do the actual work.
 * <p/>
 * This class is thread-safe!
 */
public class SamsaScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(SamsaScheduler.class);

    private final int threads;
    private final String threadNamePrefix;
    private final boolean daemon;

    private final AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private volatile ScheduledThreadPoolExecutor executor;

    public SamsaScheduler(int threads) {
        this(threads, "samsa-scheduler-", true);
    }

    public SamsaScheduler(int threads, final String threadNamePrefix) {
        this(threads, threadNamePrefix, true);
    }

    /**
     * @param threads          The number of threads in the thread pool
     * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
     * @param daemon           If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
     */
    public SamsaScheduler(final int threads, final String threadNamePrefix, boolean daemon) {
        this.threads = threads;
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    @Override
    public void startup() {
        LOG.debug("Initializing task scheduler.");
        synchronized (this) {
            if (executor != null) {
                throw new IllegalStateException("This scheduler has already been started!");
            }
            executor = new ScheduledThreadPoolExecutor(threads);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setThreadFactory(new ThreadFactory() {
                public Thread newThread(final Runnable runnable) {
                    return Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon);
                }
            });
        }
    }

    @Override
    public void shutdown() {
        LOG.debug("Shutting down task scheduler.");
        ensureStarted();
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        executor = null;
    }

    @Override
    public void schedule(final String name, final Runnable runnable, final long delay, final long period, final TimeUnit unit) {
        LOG.debug(String.format("Scheduling task %s with initial delay %d ms and period %d ms.",
                name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)));

        ensureStarted();

        final Runnable runnableWrapper = new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.trace(String.format("Begining execution of scheduled task '%s'.", name));
                    runnable.run();
                } catch (Throwable t) {
                    LOG.error("Uncaught exception in scheduled task '" + name + "'", t);
                } finally {
                    LOG.trace(String.format("Completed execution of scheduled task '%s'.", name));
                }
            }
        };

        if (period >= 0) {
            executor.scheduleAtFixedRate(runnableWrapper, delay, period, unit);
        } else {
            executor.schedule(runnableWrapper, delay, unit);
        }
    }

    private void ensureStarted() {
        if (executor == null) {
            throw new IllegalStateException("Samsa scheduler has not been started");
        }
    }
}
