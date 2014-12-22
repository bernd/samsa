package com.github.bernd.samsa;

import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.InvalidMessageSizeException;
import com.github.bernd.samsa.message.MessageSetSizeTooLargeException;
import com.github.bernd.samsa.message.MessageSizeTooLargeException;
import com.github.bernd.samsa.utils.MockTime;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class LogManagerTest {
    private MockTime time;
    private int maxRollInterval = 100;
    private long maxLogAgeMs = 10 * 60 * 60 * 1000;
    private final LogConfigBuilder logConfigBuilder = new LogConfigBuilder().segmentSize(1024).maxIndexSize(4096).retentionMs(maxLogAgeMs);
    private final LogConfig logConfig = logConfigBuilder.build();
    private File logDir = null;
    private LogManager logManager = null;
    private String name = "samsa";
    private long veryLargeLogFlushInterval = 10000000L;

    @BeforeMethod
    public void setUp() throws Throwable {
        time = new MockTime();
        logDir = TestUtils.tempDir();
        logManager = createLogManager(Lists.newArrayList(logDir));
        logManager.startup();
        logDir = logManager.getLogDirs().get(0);
    }

    @AfterMethod
    public void tearDown() throws Throwable {
        if (logManager != null) {
            logManager.shutdown();
        }
        Utils.rm(logDir);
        for (final File dir : logManager.getLogDirs()) {
            Utils.rm(dir);
        }
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     */
    @Test
    public void testCreateLog() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
        final Log log = logManager.createLog(new TopicAndPartition(name, 0), logConfig);
        final File logFile = new File(logDir, name + "-0");
        assertTrue(logFile.exists());
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test that get on a non-existent returns None and no log is created.
     */
    @Test
    public void testGetNonExistentLog() throws Exception {
        final Optional<Log> log = logManager.getLog(new TopicAndPartition(name, 0));
        assertFalse(log.isPresent(), "No log should be found.");
        final File logFile = new File(logDir, name + "-0");
        assertTrue(!logFile.exists());
    }

    /**
     * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
     */
    @Test
    public void testCleanupExpiredSegments() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetOutOfRangeException {
        final Log log = logManager.createLog(new TopicAndPartition(name, 0), logConfig);
        long offset = 0L;
        for (int i = 0; i < 200; i++) {
            final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            final LogAppendInfo info = log.append(set);
            offset = info.lastOffset;
        }
        assertTrue(log.numberOfSegments() > 1, "There should be more than one segment now.");

        for (final LogSegment segment : log.logSegments()) {
            segment.getLog().getFile().setLastModified(time.milliseconds());
        }

        time.sleep(maxLogAgeMs + 1);
        assertEquals(log.numberOfSegments(), 1, "Now there should only be only one segment in the index.");
        time.sleep(log.getConfig().getFileDeleteDelayMs() + 1);
        assertEquals(log.getDir().list().length, log.numberOfSegments() * 2, "Files should have been deleted");
        assertEquals(log.read(offset + 1, 1024).getMessageSet().sizeInBytes(), 0, "Should get empty fetch off new log.");

        try {
            log.read(0, 1024);
            assertTrue(false, "Should get exception from fetching earlier.");
        } catch (OffsetOutOfRangeException e) {
            // This is good.
        }
        // log should still be appendable
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
     */
    @Test
    public void testCleanupSegmentsToMaintainSize() throws Throwable {
        final int setSize = TestUtils.singleMessageSet("test".getBytes()).sizeInBytes();
        logManager.shutdown();

        final LogConfig config = logConfigBuilder.segmentSize(10 * setSize).retentionSize(5L * 10L * setSize + 10L).build();
        logManager = createLogManager(Lists.newArrayList(logDir));
        logManager.startup();

        // create a log
        final Log log = logManager.createLog(new TopicAndPartition(name, 0), config);
        long offset = 0L;

        // add a bunch of messages that should be larger than the retentionSize
        final int numMessages = 200;
        for(int i = 0; i < numMessages; i++) {
            final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            final LogAppendInfo info = log.append(set);
            offset = info.firstOffset;
        }

        assertEquals(log.numberOfSegments(), (numMessages * setSize) / config.getSegmentSize(), "Check we have the expected number of segments.");

        // this cleanup shouldn't find any expired segments but should delete some to reduce size
        time.sleep(LogManager.INITIAL_TASK_DELAY_MS);
        assertEquals(log.numberOfSegments(), 6, "Now there should be exactly 6 segments");
        time.sleep(log.getConfig().getFileDeleteDelayMs() + 1);
        assertEquals(log.getDir().list().length, log.numberOfSegments() * 2, "Files should have been deleted");
        assertEquals(log.read(offset + 1, 1024).getMessageSet().sizeInBytes(), 0, "Should get empty fetch off new log.");
        try {
            log.read(0, 1024);
            assertTrue(false, "Should get exception from fetching earlier.");
        } catch (OffsetOutOfRangeException e) {
            // This is good.
        }
        // log should still be appendable
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test that flush is invoked by the background scheduler thread.
     */
    @Test
    public void testTimeBasedFlush() throws Throwable {
        logManager.shutdown();
        final LogConfig config = logConfigBuilder.flushMs(1000).build();
        logManager = createLogManager(Lists.newArrayList(logDir));
        logManager.startup();
        final Log log = logManager.createLog(new TopicAndPartition(name, 0), config);
        final long lastFlush = log.lastFlushTime();
        for(int i = 0; i < 200; i++) {
            final ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            log.append(set);
        }
        time.sleep(LogManager.INITIAL_TASK_DELAY_MS);
        assertTrue(lastFlush != log.lastFlushTime(), "Time based flush should have been triggered triggered");
    }

    /**
     * Test that new logs that are created are assigned to the least loaded log directory
     */
    @Test
    public void testLeastLoadedAssignment() throws Throwable {
        // create a log manager with multiple data directories
        final List<File> dirs = Lists.newArrayList(TestUtils.tempDir(),
                TestUtils.tempDir(),
                TestUtils.tempDir());
        logManager.shutdown();
        logManager = createLogManager(dirs);

        // verify that logs are always assigned to the least loaded partition
        for(int partition = 0; partition < 20; partition++) {
            logManager.createLog(new TopicAndPartition("test", partition), logConfig);
            assertEquals(logManager.allLogs().size(), partition + 1, "We should have created the right number of logs");

            final Map<String, Integer> grouped = Maps.newHashMap();
            for (final Log log : logManager.allLogs()) {
                final String parent = log.getDir().getParent();
                if (!grouped.containsKey(parent)) {
                    grouped.put(parent, 0);
                }
                grouped.put(parent, grouped.get(parent) + 1);
            }

            final Collection<Integer> counts = grouped.values();
            assertTrue(Collections.max(counts) <= Collections.min(counts) + 1, "Load should balance evenly");
        }
    }

    /**
     * Test that it is not possible to open two log managers using the same data directory
     */
    @Test(expectedExceptions = SamsaException.class)
    public void testTwoLogManagersUsingSameDirFails() throws Throwable {
        createLogManager(Lists.newArrayList(logDir));
        assertTrue(false, "Should not be able to create a second log manager instance with the same data directory");
    }

    /**
     * Test that recovery points are correctly written out to disk
     */
    @Test
    public void testCheckpointRecoveryPoints() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
        verifyCheckpointRecovery(Lists.newArrayList(new TopicAndPartition("test-a", 1), new TopicAndPartition("test-b", 1)), logManager);
    }

    /**
     * Test that recovery points directory checking works with trailing slash
     */
    @Test
    public void testRecoveryDirectoryMappingWithTrailingSlash() throws Throwable {
        logManager.shutdown();
        logDir = TestUtils.tempDir();
        logManager = TestUtils.createLogManager(
                Lists.newArrayList(new File(logDir.getAbsolutePath() + File.separator)), logConfig, time);
        logManager.startup();
        verifyCheckpointRecovery(Lists.newArrayList(new TopicAndPartition("test-a", 1)), logManager);
    }

    /**
     * Test that recovery points directory checking works with relative directory
     */
    @Test
    public void testRecoveryDirectoryMappingWithRelativeDirectory() throws Throwable {
        logManager.shutdown();
        logDir = new File("data" + File.separator + logDir.getName());
        logDir.mkdirs();
        logDir.deleteOnExit();
        logManager = createLogManager(Lists.newArrayList(logDir));
        logManager.startup();
        verifyCheckpointRecovery(Lists.newArrayList(new TopicAndPartition("test-a", 1)), logManager);
    }

    private void verifyCheckpointRecovery(final List<TopicAndPartition> topicAndPartitions, final LogManager logManager) throws IOException, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException {
        final List<Log> logs = Lists.newArrayList();

        for (final TopicAndPartition topicAndPartition : topicAndPartitions) {
            logs.add(logManager.createLog(topicAndPartition, logConfig));
        }

        for (final Log log : logs) {
            for (int i = 0; i < 50; i++) {
                log.append(TestUtils.singleMessageSet("test".getBytes()));

                log.flush();
            }
        }


        logManager.checkpointRecoveryPointOffsets();
        final Map<TopicAndPartition, Long> checkpoints = new OffsetCheckpoint(new File(logDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE)).read();

        for (int i = 0; i < topicAndPartitions.size(); i++) {
            final Log log = logs.get(i);
            final TopicAndPartition topicAndPartition = topicAndPartitions.get(i);
            assertEquals(log.getRecoveryPoint(), (long) checkpoints.get(topicAndPartition), "Recovery point should equal checkpoint");
        }
    }

    private LogManager createLogManager(final List<File> logDirs) throws Throwable {
        return TestUtils.createLogManager(logDirs, logConfig, time);
    }
}
