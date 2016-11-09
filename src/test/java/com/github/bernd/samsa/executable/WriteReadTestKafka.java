package com.github.bernd.samsa.executable;


import com.github.bernd.samsa.utils.Utils;
import com.github.joschi.jadconfig.util.Size;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;
import kafka.common.TopicAndPartition;
import kafka.log.*;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.server.BrokerState;
import kafka.server.FetchDataInfo;
import kafka.utils.KafkaScheduler;
import kafka.utils.Time;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Map$;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static com.github.joschi.jadconfig.util.Size.megabytes;
import static java.util.concurrent.TimeUnit.*;

public class WriteReadTestKafka {
    private static final Logger LOG = LoggerFactory.getLogger(WriteReadTestKafka.class);

    public static void main(String[] args) throws Throwable {
        final ArrayList<File> logDirs = Lists.newArrayList(new File("/tmp/samsa-test"));
        final LogConfig logConfig =
                new LogConfig(
                        // segmentSize: The soft maximum for the size of a segment file in the log
                        Ints.saturatedCast(Size.megabytes(100L).toBytes()),
                        // segmentMs: The soft maximum on the amount of time before a new log segment is rolled
                        Duration.standardHours(1).getMillis(),
                        // segmentJitterMs The maximum random jitter subtracted from segmentMs to avoid thundering herds of segment rolling
                        0,
                        // flushInterval: The number of messages that can be written to the log before a flush is forced
                        1_000_000,
                        // flushMs: The amount of time the log can have dirty data before a flush is forced
                        Duration.standardMinutes(1).getMillis(),
                        // retentionSize: The approximate total number of bytes this log can use
                        Size.kilobytes(10L).toBytes(),
                        // retentionMs: The age approximate maximum age of the last segment that is retained
                        Duration.standardDays(1).getMillis(),
                        // maxMessageSize: The maximum size of a message in the log
                        Integer.MAX_VALUE,
                        // maxIndexSize: The maximum size of an index file
                        Ints.saturatedCast(megabytes(1L).toBytes()),
                        // indexInterval: The approximate number of bytes between index entries
                        4096,
                        // fileDeleteDelayMs: The time to wait before deleting a file from the filesystem
                        MINUTES.toMillis(1L),
                        // deleteRetentionMs: The time to retain delete markers in the log. Only applicable for logs that are being compacted.
                        DAYS.toMillis(1L),
                        // minCleanableRatio: The ratio of bytes that are available for cleaning to the bytes already cleaned
                        0.5,
                        // compact: Should old segments in this log be deleted or de-duplicated?
                        false,
                        // uncleanLeaderElectionEnable Indicates whether unclean leader election is enabled; actually a controller-level property
                        //                             but included here for topic-specific configuration validation purposes
                        true,
                        // minInSyncReplicas If number of insync replicas drops below this number, we stop accepting writes with -1 (or all) required acks
                        0
                );


        final CleanerConfig cleanerConfig =
                new CleanerConfig(
                        1,
                        Size.megabytes(4L).toBytes(),
                        0.9d,
                        Ints.saturatedCast(Size.megabytes(1L).toBytes()),
                        Ints.saturatedCast(Size.megabytes(32L).toBytes()),
                        Ints.saturatedCast(Size.megabytes(5L).toBytes()),
                        SECONDS.toMillis(15L),
                        false,
                        "MD5");


        final int ioThreads = 2;
        final long flushCheckMs = TimeUnit.SECONDS.toMillis(60);
        final long flushCheckpointMs = TimeUnit.SECONDS.toMillis(60);
        final long retentionCheckMs = TimeUnit.SECONDS.toDays(20);

        final KafkaScheduler scheduler = new KafkaScheduler(2, "kafka-journal-scheduler-", false); // TODO make thread count configurable
        scheduler.startup();


        final BrokerState brokerState = new BrokerState();


        Time time = new Time() {
            @Override
            public long milliseconds() {
                return DateTimeUtils.currentTimeMillis();
            }

            @Override
            public long nanoseconds() {
                return System.nanoTime();
            }

            @Override
            public void sleep(long ms) {
                Uninterruptibles.sleepUninterruptibly(ms, MILLISECONDS);
            }
        };

        final LogManager logManager = new LogManager(logDirs.toArray(new File[logDirs.size()]), Map$.MODULE$.<String, LogConfig>empty(), logConfig, cleanerConfig, ioThreads,
                flushCheckMs, flushCheckpointMs, retentionCheckMs, scheduler, brokerState, time);

        final TopicAndPartition topicAndPartition = new TopicAndPartition("test", 0);
        final Option<Log> logOptional = logManager.getLog(topicAndPartition);
        final Log log;

        if (!logOptional.isEmpty()) {
            log = logOptional.get();
        } else {
            log = logManager.createLog(topicAndPartition, logConfig);
        }

        LOG.info("Initialized log at {}", logDirs.toArray(new File[logDirs.size()]));

        final Log.LogAppendInfo info = log.append(new ByteBufferMessageSet(JavaConversions.asScalaBuffer(Lists.newArrayList(new Message("hello world".getBytes())))), true);

        //LOG.info("Wrote message firstOffset={} lastOffset={} validBytes={}", info.firstOffset(), info.lastOffset(), info.validBytes());


        //read section


        final Iterable<LogSegment> logSegments =
                JavaConversions.asJavaIterable(log.logSegments());
        final LogSegment segment = Iterables.getFirst(logSegments, null);
        long logStartOffset = 0;

        if (segment != null) {
            logStartOffset = segment.baseOffset();
        }


        final FetchDataInfo fetchDataInfo = log.read(logStartOffset,  5 * 1024 * 1024, Option.<Object>apply(new Long(37)));
        final MessageSet messageSet = fetchDataInfo.messageSet();

        Iterator<MessageAndOffset> messageIterator = messageSet.iterator();

        while (messageIterator.hasNext()) {
            MessageAndOffset item = messageIterator.next();
            final Message message = item.message();
            final long offset = item.offset();
            LOG.info("Read message: \"{}\" (at {})", new String(Utils.readBytes(message.payload())), offset);
        }

        log.flush();
        log.close();
        logManager.shutdown();
    }
}
