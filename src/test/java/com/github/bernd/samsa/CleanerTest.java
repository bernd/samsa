package com.github.bernd.samsa;

import com.github.bernd.samsa.cleaner.CheckDoneCallback;
import com.github.bernd.samsa.cleaner.Cleaner;
import com.github.bernd.samsa.cleaner.LogCleaningAbortedException;
import com.github.bernd.samsa.cleaner.LogToClean;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.InvalidMessageSizeException;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSetSizeTooLargeException;
import com.github.bernd.samsa.message.MessageSizeTooLargeException;
import com.github.bernd.samsa.utils.MockTime;
import com.github.bernd.samsa.utils.Throttler;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Unit tests for the log cleaning logic
 */
public class CleanerTest {
    private MockTime time;
    private Throttler throttler;
    private LogConfig logConfig;
    private File dir;

    @BeforeMethod
    public void setUp() throws Exception {
        dir = TestUtils.tempDir();
        logConfig = new LogConfig(1024,
                LogConfig.Defaults.SEGMENT_MS,
                LogConfig.Defaults.SEGMENT_JITTER_MS,
                LogConfig.Defaults.FLUSH_INTERVAL,
                LogConfig.Defaults.FLUSH_MS,
                LogConfig.Defaults.RETENTION_SIZE,
                LogConfig.Defaults.RETENTION_MS,
                LogConfig.Defaults.MAX_MESSAGE_SIZE,
                1024,
                LogConfig.Defaults.INDEX_INTERVAL,
                LogConfig.Defaults.FILE_DELETE_DELAY_MS,
                LogConfig.Defaults.DELETE_RETENTION_MS,
                LogConfig.Defaults.MIN_CLEANABLE_DIRTY_RATIO,
                true,
                LogConfig.Defaults.UNCLEAN_LEADER_ELECTION_ENABLE,
                LogConfig.Defaults.MIN_IN_SYNC_REPLICAS);
        time = new MockTime();
        throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE, true, time);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Utils.rm(dir);
    }

    /**
     * Test simple log cleaning
     */
    @Test
    public void testCleanSegments() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetMapException, LogCleaningAbortedException {
        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
            @Override
            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
                // ignore
            }
        });
        final Log log = makeLog(dir, logConfig); // Segment size needs to be 1024!

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4) {
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
        }
        final Iterable<Integer> keysFound = keysInLog(log);
        final List<Integer> keysExpected = Lists.newArrayList();

        for (int i = 0; i < log.logEndOffset(); i++) {
            keysExpected.add(i);
        }

        assertEquals(keysFound, keysExpected);

        // pretend we have the following keys
        final List<Integer> keys = Lists.newArrayList(1, 3, 5, 7, 9);
        final OffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);

        for (final int k : keys) {
            map.put(key(k), Long.MAX_VALUE);
        }

        // clean the log
        final Iterator<LogSegment> iterator = log.logSegments().iterator();
        final List<LogSegment> logSegmentsToClean = Lists.newArrayList(iterator.next(), iterator.next(), iterator.next());
        cleaner.cleanSegments(log, logSegmentsToClean, map, 0L);

        final Iterable<Integer> shouldRemain = Iterables.filter(keysInLog(log), new Predicate<Integer>() {
            @Override
            public boolean apply(Integer i) {
                return !keys.contains(i);
            }
        });

        assertEquals(keysInLog(log), shouldRemain);
    }

    @Test
    public void testCleaningWithDeletes() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, LogCleaningAbortedException, OffsetMapException {
        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
            @Override
            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
                // ignore
            }
        });
        final Log log = makeLog(dir, logConfig); // Segment size needs to be 1024!

        // append messages with the keys 0 through N
        while (log.numberOfSegments() < 2) {
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
        }

        // delete all even keys between 0 and N
        final long leo = log.logEndOffset();
        for (int i = 0; i < (int) leo; i += 2) {
            log.append(deleteMessage(i));
        }

        // append some new unique keys to pad out to a new active segment
        while (log.numberOfSegments() < 4) {
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
        }

        cleaner.clean(new LogToClean(new TopicAndPartition("test", 0), log, 0));

        final Set<Integer> keys = Sets.newHashSet(keysInLog(log));

        // None of the keys we deleted should still exist.
        for (int i = 0; i < (int) leo; i += 2) {
            assertFalse(keys.contains(i));
        }
    }

    /* extract all the keys from a log */
    private Iterable<Integer> keysInLog(final Log log) throws UnsupportedEncodingException {
        final List<Integer> list = Lists.newArrayList();

        for (final LogSegment segment : log.logSegments()) {
            for (MessageAndOffset messageAndOffset : segment.getLog()) {
                if (messageAndOffset.getMessage().isNull()) {
                    continue;
                }

                list.add(Integer.parseInt(Utils.readString(messageAndOffset.getMessage().key())));
            }
        }

        return list;
    }

    public void abortCheckDone(final TopicAndPartition topicAndPartition) throws LogCleaningAbortedException {
        throw new LogCleaningAbortedException();
    }

    /**
     * Test that abortion during cleaning throws a LogCleaningAbortedException
     */
    @Test(expectedExceptions = LogCleaningAbortedException.class)
    public void testCleanSegmentsWithAbort() throws Exception, MessageSetSizeTooLargeException, SamsaStorageException, MessageSizeTooLargeException, InvalidMessageSizeException, OffsetMapException, LogCleaningAbortedException {
        final Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, new CheckDoneCallback<TopicAndPartition>() {
            @Override
            public void call(TopicAndPartition argument) throws LogCleaningAbortedException {
                abortCheckDone(argument);
            }
        });
        final Log log = makeLog(dir, logConfig); // Segment size needs to be 1024!

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4) {
            log.append(message((int) log.logEndOffset(), (int) log.logEndOffset()));
        }

        final Iterable<Integer> keys = keysInLog(log);
        final OffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);

        for (final int k : keys) {
            map.put(key(k), Long.MAX_VALUE);
        }

        final Iterator<LogSegment> iterator = log.logSegments().iterator();
        final List<LogSegment> logSegmentsToClean = Lists.newArrayList(iterator.next(), iterator.next(), iterator.next());

        cleaner.cleanSegments(log, logSegmentsToClean, map, 0L);
    }

    public Log makeLog(final File dir, final LogConfig config) throws IOException {
        return new Log(dir, config, 0L, time.scheduler);
    }

    private Cleaner makeCleaner(final int capacity, final CheckDoneCallback<TopicAndPartition> checkDone) {
        return new Cleaner(0,
                new FakeOffsetMap(capacity),
                64 * 1024,
                64 * 1024,
                0.75,
                throttler,
                time, checkDone);
    }

    public ByteBuffer key(final int id) {
        return ByteBuffer.wrap(String.valueOf(id).getBytes());
    }

    public ByteBufferMessageSet message(final int key, final int value) throws IOException {
        return new ByteBufferMessageSet(
                Lists.newArrayList(
                        new Message(String.valueOf(key).getBytes(), String.valueOf(value).getBytes())));
    }

    private ByteBufferMessageSet deleteMessage(final int key) throws IOException {
        return new ByteBufferMessageSet(Lists.newArrayList(new Message(null, String.valueOf(key).getBytes())));
    }

    private class FakeOffsetMap implements OffsetMap {
        private final Map<String, Long> map = Maps.newHashMap();

        public FakeOffsetMap(int capacity) {
        }

        private String keyFor(final ByteBuffer key) {
            return new String(Utils.readBytes(key.duplicate()), Charsets.UTF_8);
        }

        @Override
        public int getSlots() {
            return 0;
        }

        @Override
        public void put(ByteBuffer key, long offset) throws OffsetMapException {
            map.put(keyFor(key), offset);
        }

        @Override
        public long get(ByteBuffer key) throws OffsetMapException {
            final String k = keyFor(key);

            return map.containsKey(k) ? map.get(k) : -1L;
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public int getSize() {
            return map.size();
        }

        @Override
        public double getUtilization() {
            return 0;
        }
    }
}
