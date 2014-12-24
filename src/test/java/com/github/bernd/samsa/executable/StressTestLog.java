package com.github.bernd.samsa.executable;

import com.github.bernd.samsa.Log;
import com.github.bernd.samsa.LogAppendInfo;
import com.github.bernd.samsa.LogConfig;
import com.github.bernd.samsa.LogConfigBuilder;
import com.github.bernd.samsa.OffsetOutOfRangeException;
import com.github.bernd.samsa.SamsaStorageException;
import com.github.bernd.samsa.TestUtils;
import com.github.bernd.samsa.message.FileMessageSet;
import com.github.bernd.samsa.message.InvalidMessageSizeException;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.message.MessageSetSizeTooLargeException;
import com.github.bernd.samsa.message.MessageSizeTooLargeException;
import com.github.bernd.samsa.utils.MockTime;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stress test that instantiates a log and then runs continual appends against it from one thread and continual reads against it
 * from another thread and checks a few basic assertions until the user kills the process.
 */
public class StressTestLog {
    private static final Logger LOG = LoggerFactory.getLogger(StressTestLog.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) throws IOException {
        final File dir = TestUtils.tempDir();
        final MockTime time = new MockTime();
        final LogConfig logConfig = new LogConfigBuilder().segmentSize(64 * 1024 * 1024)
                .maxMessageSize(Integer.MAX_VALUE).maxIndexSize(1024 * 1024).build();
        final Log log = new Log(dir, logConfig, 0L, time.scheduler, time);

        final WriterThread writer = new WriterThread(log);
        writer.start();
        final ReaderThread reader = new ReaderThread(log);
        reader.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                running.set(false);
                try {
                    writer.join();
                    reader.join();
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage(), e);
                }
                Utils.rm(dir);
            }
        });

        while (running.get()) {
            LOG.info("Reader offset = {}, writer offset = {}", reader.offset, writer.offset);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static abstract class WorkerThread extends Thread {
        @Override
        public void run() {
            try {
                int offset = 0;
                while (running.get()) {
                    work();
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                running.set(false);
            }
            LOG.info("{} exiting...", getClass().getName());
        }

        public abstract void work();
    }

    private static class WriterThread extends WorkerThread {
        private final Log log;
        private volatile int offset = 0;

        public WriterThread(final Log log) {
            this.log = log;
        }

        @Override
        public void work() {
            try {
                final LogAppendInfo logAppendInfo = log.append(TestUtils.singleMessageSet(String.valueOf(offset).getBytes()));
                Utils.require(logAppendInfo.firstOffset == offset && logAppendInfo.lastOffset == offset);
                offset += 1;
                if (offset % 1000 == 0) {
                    Thread.sleep(500);
                }
            } catch (SamsaStorageException | MessageSetSizeTooLargeException | InvalidMessageSizeException | MessageSizeTooLargeException | InterruptedException | IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static class ReaderThread extends WorkerThread {
        private final Log log;
        private volatile int offset = 0;

        public ReaderThread(final Log log) {
            this.log = log;
        }

        @Override
        public void work() {
            try {
                final MessageSet read = log.read(offset, 1024, Optional.of(offset + 1L)).getMessageSet();

                if (read instanceof FileMessageSet && read.sizeInBytes() > 0) {
                    final MessageAndOffset first = Iterables.get(read, 0);
                    Utils.require(first.getOffset() == offset,
                            "We should either read nothing or the message we asked for.");
                    Utils.require(
                            MessageSet.entrySize(first.getMessage()) == read.sizeInBytes(),
                            String.format("Expected %d but got %d.",
                                    MessageSet.entrySize(first.getMessage()), read.sizeInBytes()));
                    offset += 1;
                }
            } catch (OffsetOutOfRangeException e) {
                // this is okay
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
