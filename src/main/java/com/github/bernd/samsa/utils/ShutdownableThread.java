package com.github.bernd.samsa.utils;

import com.github.bernd.samsa.OffsetMapException;
import com.github.bernd.samsa.SamsaStorageException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownableThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownableThread.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final boolean isInterruptible;

    public ShutdownableThread(final String name) {
        this(name, true);
    }

    public ShutdownableThread(final String name, final boolean isInterruptible) {
        super(name);
        this.isInterruptible = isInterruptible;

        setDaemon(false);
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public boolean initiateShutdown() {
        if (isRunning.compareAndSet(true, false)) {
            LOG.info("Shutting down");
            isRunning.set(false);
            if (isInterruptible) {
                interrupt();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown() {
        Uninterruptibles.awaitUninterruptibly(shutdownLatch);
        LOG.info("Shutdown completed");
    }

    public abstract void doWork() throws SamsaStorageException, IOException, OffsetMapException;

    @Override
    public void run() {
        LOG.info("Starting ");
        try {
            while (isRunning.get()) {
                doWork();
            }
        } catch (Throwable e) {
            if (isRunning.get()) {
                LOG.error("Error due to ", e);
            }
        }
        shutdownLatch.countDown();
        LOG.info("Stopped ");
    }

    public AtomicBoolean getIsRunning() {
        return isRunning;
    }
}
