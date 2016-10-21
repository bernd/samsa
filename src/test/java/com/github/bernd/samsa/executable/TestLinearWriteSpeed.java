package com.github.bernd.samsa.executable;

import com.github.bernd.samsa.Log;
import com.github.bernd.samsa.LogConfig;
import com.github.bernd.samsa.LogConfigBuilder;
import com.github.bernd.samsa.SamsaStorageException;
import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.InvalidMessageSizeException;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.message.MessageSetSizeTooLargeException;
import com.github.bernd.samsa.message.MessageSizeTooLargeException;
import com.github.bernd.samsa.utils.CommandLineUtils;
import com.github.bernd.samsa.utils.SamsaScheduler;
import com.github.bernd.samsa.utils.Scheduler;
import com.github.bernd.samsa.utils.SystemTime;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.collect.Lists;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Random;

public class TestLinearWriteSpeed {
    private static final Logger LOG = LoggerFactory.getLogger(TestLinearWriteSpeed.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        final OptionParser parser = new OptionParser();
        final ArgumentAcceptingOptionSpec<String> dirOpt = parser.accepts("dir", "The directory to write to.")
                .withRequiredArg()
                .describedAs("path")
                .ofType(String.class)
                .defaultsTo(System.getProperty("java.io.tmpdir"));
        final ArgumentAcceptingOptionSpec<Long> bytesOpt = parser.accepts("bytes", "REQUIRED: The total number of bytes to write.")
                .withRequiredArg()
                .describedAs("num_bytes")
                .ofType(Long.class);
        final ArgumentAcceptingOptionSpec<Integer> sizeOpt = parser.accepts("size", "REQUIRED: The size of each write.")
                .withRequiredArg()
                .describedAs("num_bytes")
                .ofType(Integer.class);
        final ArgumentAcceptingOptionSpec<Integer> messageSizeOpt = parser.accepts("message-size", "REQUIRED: The size of each message in the message set.")
                .withRequiredArg()
                .describedAs("num_bytes")
                .ofType(Integer.class)
                .defaultsTo(1024);
        final ArgumentAcceptingOptionSpec<Integer> filesOpt = parser.accepts("files", "REQUIRED: The number of logs or files.")
                .withRequiredArg()
                .describedAs("num_files")
                .ofType(Integer.class)
                .defaultsTo(1);
        final ArgumentAcceptingOptionSpec<Long> reportingIntervalOpt = parser.accepts("reporting-interval", "The number of ms between updates.")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Long.class)
                .defaultsTo(1000L);
        final ArgumentAcceptingOptionSpec<Integer> maxThroughputOpt = parser.accepts("max-throughput-mb", "The maximum throughput.")
                .withRequiredArg()
                .describedAs("mb")
                .ofType(Integer.class)
                .defaultsTo(Integer.MAX_VALUE);
        final ArgumentAcceptingOptionSpec<Long> flushIntervalOpt = parser.accepts("flush-interval", "The number of messages between flushes")
                .withRequiredArg()
                .describedAs("message_count")
                .ofType(Long.class)
                .defaultsTo(Long.MAX_VALUE);
        final ArgumentAcceptingOptionSpec<String> compressionCodecOpt = parser.accepts("compression", "The compression codec to use")
                .withRequiredArg()
                .describedAs("codec")
                .ofType(String.class)
                .defaultsTo(CompressionCodec.NONE.name());
        final OptionSpecBuilder mmapOpt = parser.accepts("mmap", "Do writes to memory-mapped files.");
        final OptionSpecBuilder channelOpt = parser.accepts("channel", "Do writes to file channels.");
        final OptionSpecBuilder logOpt = parser.accepts("log", "Do writes to samsa logs.");

        final OptionSet options = parser.parse(args);

        CommandLineUtils.checkRequiredArgs(parser, options, bytesOpt, sizeOpt, filesOpt);

        long bytesToWrite = options.valueOf(bytesOpt);
        final int bufferSize = options.valueOf(sizeOpt);
        final int numFiles = options.valueOf(filesOpt);
        final long reportingInterval = options.valueOf(reportingIntervalOpt);
        final String dir = options.valueOf(dirOpt);
        final long maxThroughputBytes = options.valueOf(maxThroughputOpt) * 1024L * 1024L;
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        final int messageSize = options.valueOf(messageSizeOpt);
        final long flushInterval = options.valueOf(flushIntervalOpt);
        final CompressionCodec compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOpt));
        final Random rand = new Random();
        rand.nextBytes(buffer.array());
        final int numMessages = bufferSize / (messageSize + MessageSet.LOG_OVERHEAD);

        final List<Message> messages = Lists.newArrayList();
        for (int i = 0; i < numMessages; i++) {
            messages.add(new Message(new byte[messageSize]));
        }

        final ByteBufferMessageSet messageSet = new ByteBufferMessageSet(compressionCodec, messages);

        final Writable[] writables = new Writable[numFiles];
        final Scheduler scheduler = new SamsaScheduler(1);
        scheduler.startup();
        for (int i = 0; i < numFiles; i++) {
            if (options.has(mmapOpt)) {
                writables[i] = new MmapWritable(new File(dir, "samsa-test-" + i + ".dat"), bytesToWrite / numFiles, buffer);
            } else if (options.has(channelOpt)) {
                writables[i] = new ChannelWritable(new File(dir, "samsa-test-" + i + ".dat"), buffer);
            } else if (options.has(logOpt)) {
                final int segmentSize = rand.nextInt(512) * 1024 * 1024 + 64 * 1024 * 1024; // vary size to avoid herd effect
                writables[i] = new LogWritable(new File(dir, "samsa-test-" + i),
                        new LogConfigBuilder().segmentSize(segmentSize).flushInterval(flushInterval).build(),
                        scheduler,
                        messageSet);
            } else {
                System.out.println("Must specify what to write to with one of --log, --channel, or --mmap");
                System.exit(1);
            }
        }
        bytesToWrite = (bytesToWrite / numFiles) * numFiles;

        System.out.println(String.format("%10s\t%10s\t%10s", "mb_sec", "avg_latency", "max_latency"));

        final long beginTest = System.nanoTime();
        long maxLatency = 0L;
        long totalLatency = 0L;
        long count = 0L;
        long written = 0L;
        long totalWritten = 0L;
        long lastReport = beginTest;
        while (totalWritten + bufferSize < bytesToWrite) {
            final long start = System.nanoTime();
            final int writeSize = writables[Math.abs((int) (count % numFiles))].write();
            final long ellapsed = System.nanoTime() - start;
            maxLatency = Math.max(ellapsed, maxLatency);
            totalLatency += ellapsed;
            written += writeSize;
            count += 1;
            totalWritten += writeSize;
            if ((start - lastReport) / (1000.0 * 1000.0) > (double) reportingInterval) {
                final double ellapsedSecs = (start - lastReport) / (1000.0 * 1000.0 * 1000.0);
                final double mb = written / (1024.0 * 1024.0);
                System.out.println(String.format("%10.3f\t%10.3f\t%10.3f", mb / ellapsedSecs,
                        totalLatency / (double) count / (1000.0 * 1000.0), maxLatency / (1000.0 * 1000.0)));
                lastReport = start;
                written = 0;
                maxLatency = 0L;
                totalLatency = 0L;
            } else if (written > maxThroughputBytes * (reportingInterval / 1000.0)) {
                // if we have written enough, just sit out this reporting interval
                final long lastReportMs = lastReport / (1000 * 1000);
                final long now = System.nanoTime() / (1000 * 1000);
                final long sleepMs = lastReportMs + reportingInterval - now;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
        }
        final double elapsedSecs = (System.nanoTime() - beginTest) / (1000.0 * 1000.0 * 1000.0);
        System.out.println(bytesToWrite / (1024.0 * 1024.0 * elapsedSecs) + " MB per sec");
        scheduler.shutdown();
    }

    private static interface Writable {
        public int write();

        public void close();
    }

    private static class MmapWritable implements Writable {
        private static final Logger LOG = LoggerFactory.getLogger(MmapWritable.class);

        private final File file;
        private final long size;
        private final ByteBuffer content;
        private final RandomAccessFile raf;
        private final MappedByteBuffer buffer;

        public MmapWritable(final File file, final long size, final ByteBuffer content) throws IOException {
            this.file = file;
            this.size = size;
            this.content = content;

            file.deleteOnExit();

            this.raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
        }

        @Override
        public int write() {
            buffer.put(content);
            content.rewind();
            return content.limit();
        }

        @Override
        public void close() {
            try {
                raf.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static class ChannelWritable implements Writable {
        private static final Logger LOG = LoggerFactory.getLogger(ChannelWritable.class);

        private final File file;
        private final ByteBuffer content;
        private final RandomAccessFile raf;
        private final FileChannel channel;

        public ChannelWritable(final File file, final ByteBuffer content) throws IOException {
            this.file = file;
            this.content = content;

            file.deleteOnExit();
            this.raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();
        }

        @Override
        public int write() {
            try {
                channel.write(content);
                content.rewind();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }

            return content.limit();
        }

        @Override
        public void close() {
            try {
                raf.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static class LogWritable implements Writable {
        private static final Logger LOG = LoggerFactory.getLogger(LogWritable.class);

        private final Log log;
        private final ByteBufferMessageSet messages;

        public LogWritable(final File dir,
                           final LogConfig config,
                           final Scheduler scheduler,
                           final ByteBufferMessageSet messages) throws IOException {

            this.messages = messages;

            Utils.rm(dir);
            this.log = new Log(dir, config, 0L, scheduler, new SystemTime());
        }

        @Override
        public int write() {
            try {
                log.append(messages, true);
            } catch (MessageSizeTooLargeException | InvalidMessageSizeException | MessageSetSizeTooLargeException | SamsaStorageException | IOException e) {
                LOG.error(e.getMessage(), e);
            }
            return messages.sizeInBytes();
        }

        @Override
        public void close() {
            log.close();
            Utils.rm(log.getDir());
        }
    }
}
