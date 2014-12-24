package com.github.bernd.samsa.executable;

import com.github.bernd.samsa.BrokerState;
import com.github.bernd.samsa.FetchDataInfo;
import com.github.bernd.samsa.Log;
import com.github.bernd.samsa.LogAppendInfo;
import com.github.bernd.samsa.LogConfig;
import com.github.bernd.samsa.LogConfigBuilder;
import com.github.bernd.samsa.LogManager;
import com.github.bernd.samsa.TopicAndPartition;
import com.github.bernd.samsa.cleaner.CleanerConfigBuilder;
import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.utils.SamsaScheduler;
import com.github.bernd.samsa.utils.SystemTime;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class WriteReadTest {
    private static final Logger LOG = LoggerFactory.getLogger(WriteReadTest.class);

    public static void main(String[] args) throws Throwable {
        final int ioThreads = 2;
        final long flushCheckMs = TimeUnit.SECONDS.toMillis(60);
        final long flushCheckpointMs = TimeUnit.SECONDS.toMillis(60);
        final long retentionCheckMs = TimeUnit.SECONDS.toDays(20);

        final LogConfig logConfig = new LogConfigBuilder().build();

        final LogManager logManager = new LogManager(Lists.newArrayList(new File("/tmp/samsa-test")),
                new HashMap<String, LogConfig>(),
                logConfig,
                new CleanerConfigBuilder().build(),
                ioThreads, flushCheckMs, flushCheckpointMs, retentionCheckMs,
                new SamsaScheduler(2),
                new BrokerState(),
                new SystemTime());

        final TopicAndPartition topicAndPartition = new TopicAndPartition("test", 0);
        final Optional<Log> logOptional = logManager.getLog(topicAndPartition);
        final Log log;

        if (logOptional.isPresent()) {
            log = logOptional.get();
        } else {
            log = logManager.createLog(topicAndPartition, logConfig);
        }

        LOG.info("Initialized log at {}", logManager.getLogDirs());

        final LogAppendInfo info = log.append(new ByteBufferMessageSet(CompressionCodec.NONE, Lists.newArrayList(new Message("hello world".getBytes()))));

        LOG.info("Wrote message firstOffset={} lastOffset={} validBytes={}", info.firstOffset, info.lastOffset, info.validBytes);

        final FetchDataInfo fetchDataInfo = log.read(info.lastOffset, info.validBytes);
        final MessageSet messageSet = fetchDataInfo.getMessageSet();

        for (final MessageAndOffset messageAndOffset : messageSet) {
            final Message message = messageAndOffset.getMessage();
            final long offset = messageAndOffset.getOffset();

            LOG.info("Read message: \"{}\" (at {})", new String(Utils.readBytes(message.payload())), offset);
        }
    }
}
