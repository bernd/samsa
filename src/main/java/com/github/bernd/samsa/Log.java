package com.github.bernd.samsa;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.InvalidMessageSizeException;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.message.MessageSetSizeTooLargeException;
import com.github.bernd.samsa.message.MessageSizeTooLargeException;
import com.github.bernd.samsa.utils.Scheduler;
import com.github.bernd.samsa.utils.Time;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An append-only log for storing messages.
 * <p/>
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 * <p/>
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 * <p/>
 * This class is thread-safe!
 */
public class Log {
    private static final Logger LOG = LoggerFactory.getLogger(Log.class);

    /**
     * a log file
     */
    public static String LOG_FILE_SUFFIX = ".log";

    /**
     * an index file
     */
    public static String INDEX_FILE_SUFFIX = ".index";

    /**
     * a file that is scheduled to be deleted
     */
    public static String DELETED_FILE_SUFFIX = ".deleted";

    /**
     * A temporary file that is being used for log cleaning
     */
    public static String CLEANED_FILE_SUFFIX = ".cleaned";

    /**
     * A temporary file used when swapping files into the log
     */
    public static String SWAP_FILE_SUFFIX = ".swap";

    /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
     * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
    /**
     * TODO: Get rid of CleanShutdownFile in 0.8.2
     */
    public static String CLEAN_SHUTDOWN_FILE = ".kafka_cleanshutdown";

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Construct a log file name in the given dir with the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFilename(final File dir, final long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LOG_FILE_SUFFIX);
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File indexFilename(final File dir, final long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + INDEX_FILE_SUFFIX);
    }

    /**
     * Parse the topic and partition out of the directory name of a log
     */
    public static TopicAndPartition parseTopicPartitionName(final File dir) {
        final String name = dir.getName();
        if (name == null || name.isEmpty() || !name.contains("-")) {
            throwException(dir);
        }
        final int index = name.lastIndexOf('-');
        final String topic = name.substring(0, index);
        final String partition = name.substring(index + 1);
        if (topic.length() < 1 || partition.length() < 1) {
            throwException(dir);
        }
        return new TopicAndPartition(topic, Integer.parseInt(partition));
    }

    private static void throwException(final File dir) {
        String canonicalPath;
        try {
            canonicalPath = dir.getCanonicalPath();
        } catch (IOException e) {
            canonicalPath = "<error: " + e.getMessage() + ">";
        }

        throw new SamsaException("Found directory " + canonicalPath + ", " +
                "'" + dir.getName() + "' is not in the form of topic-partition\n" +
                "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
                "directory");
    }

    private final File dir;
    private volatile LogConfig config;
    private volatile long recoveryPoint;
    private final Scheduler scheduler;
    private final Time time;
    private final Map<String, String> tags = new HashMap();

    /* A lock that guards all modifications to the log */
    private final Object lock = new Object();

    /* last time it was flushed */
    private final AtomicLong lastflushedTime;

    /* the actual segments of the log */
    private final ConcurrentNavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap();

    /* Calculate the offset of the next message */
    private volatile LogOffsetMetadata nextOffsetMetadata;

    private final TopicAndPartition topicAndPartition;

    /**
     * @param dir           The directory in which log segments are created.
     * @param config        The log configuration settings
     * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
     * @param scheduler     The thread pool scheduler used for background actions
     * @param time          The time instance used for checking the clock
     */
    public Log(final File dir,
               final LogConfig config, // was marked as volatile
               final long recoveryPoint,
               final Scheduler scheduler,
               final Time time) throws IOException {
        this.dir = dir;
        this.config = config;
        this.recoveryPoint = recoveryPoint;
        this.scheduler = scheduler;
        this.time = time;
        this.lastflushedTime = new AtomicLong(time.milliseconds());

        loadSegments();

        this.nextOffsetMetadata = new LogOffsetMetadata(activeSegment().nextOffset(),
                activeSegment().getBaseOffset(), (int) activeSegment().size());
        this.topicAndPartition = parseTopicPartitionName(dir);

        LOG.info(String.format("Completed load of log %s with log end offset %d", name(), logEndOffset()));

        this.tags.put("topic", topicAndPartition.getTopic());
        this.tags.put("partition", String.valueOf(topicAndPartition.getPartition()));
    }

    /**
     * The name of this log
     */
    public String name() {
        return dir.getName();
    }

    /* Load the log segments from the log files on disk */
    private void loadSegments() throws IOException {
        // create the log directory if it doesn't exist
        dir.mkdirs();

        // first do a pass through the files in the log directory and remove any temporary files
        // and complete any interrupted swap operations
        for (final File file : dir.listFiles()) {
            if (!file.isFile()) {
                continue;
            }
            if (!file.canRead()) {
                throw new IOException("Could not read file " + file);
            }
            final String filename = file.getName();
            if (filename.endsWith(DELETED_FILE_SUFFIX) || filename.endsWith(CLEANED_FILE_SUFFIX)) {
                // if the file ends in .deleted or .cleaned, delete it
                file.delete();
            } else if (filename.endsWith(SWAP_FILE_SUFFIX)) {
                // we crashed in the middle of a swap operation, to recover:
                // if a log, swap it in and delete the .index file
                // if an index just delete it, it will be rebuilt
                final File baseName = new File(Utils.replaceSuffix(file.getPath(), SWAP_FILE_SUFFIX, ""));
                if (baseName.getPath().endsWith(INDEX_FILE_SUFFIX)) {
                    file.delete();
                } else if (baseName.getPath().endsWith(LOG_FILE_SUFFIX)) {
                    // delete the index
                    final File index = new File(Utils.replaceSuffix(baseName.getPath(), LOG_FILE_SUFFIX, INDEX_FILE_SUFFIX));
                    index.delete();
                    // complete the swap operation
                    final boolean renamed = file.renameTo(baseName);
                    if (renamed) {
                        LOG.info(String.format("Found log file %s from interrupted swap operation, repairing.", file.getPath()));
                    } else {
                        throw new SamsaException(String.format("Failed to rename file %s.", file.getPath()));
                    }
                }
            }
        }

        // now do a second pass and load all the .log and .index files
        for (final File file : dir.listFiles()) {
            if (!file.isFile()) {
                continue;
            }
            final String filename = file.getName();
            if (filename.endsWith(INDEX_FILE_SUFFIX)) {
                // if it is an index file, make sure it has a corresponding .log file
                final File logFile = new File(file.getAbsolutePath().replace(INDEX_FILE_SUFFIX, LOG_FILE_SUFFIX));
                if (!logFile.exists()) {
                    LOG.warn(String.format("Found an orphaned index file, %s, with no corresponding log file.", file.getAbsolutePath()));
                    file.delete();
                }
            } else if (filename.endsWith(LOG_FILE_SUFFIX)) {
                // if its a log file, load the corresponding log segment
                final long start = Long.parseLong(filename.substring(0, filename.length() - LOG_FILE_SUFFIX.length()));
                final boolean hasIndex = Log.indexFilename(dir, start).exists();
                final LogSegment segment = new LogSegment(dir,
                        start,
                        config.getIndexInterval(),
                        config.getMaxIndexSize(),
                        config.getRandomSegmentJitter(),
                        time);
                if (!hasIndex) {
                    LOG.error(String.format("Could not find index file corresponding to log file %s, rebuilding index...", segment.getLog().getFile().getAbsolutePath()));
                    segment.recover(config.getMaxMessageSize());
                }
                segments.put(start, segment);
            }
        }

        if (Iterables.isEmpty(logSegments())) {
            // no existing segments, create a new mutable segment beginning at offset 0
            segments.put(0L, new LogSegment(dir,
                    0,
                    config.getIndexInterval(),
                    config.getMaxIndexSize(),
                    config.getRandomSegmentJitter(),
                    time));
        } else {
            recoverLog();
            // reset the index size of the currently active log segment to allow more entries
            activeSegment().getIndex().resize(config.getMaxIndexSize());
        }

        // sanity check the index file of every segment to ensure we don't proceed with a corrupt segment
        for (LogSegment logSegment : logSegments()) {
            logSegment.getIndex().sanityCheck();
        }
    }

    private void updateLogEndOffset(final long messageOffset) {
        nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment().getBaseOffset(), (int) activeSegment().size());
    }

    private void recoverLog() throws IOException {
        // if we have the clean shutdown marker, skip recovery
        if (hasCleanShutdownFile()) {
            recoveryPoint = activeSegment().nextOffset();
            return;
        }

        // okay we need to actually recovery this log
        final Iterator<LogSegment> unflushed = logSegments(recoveryPoint, Long.MAX_VALUE).iterator();
        while (unflushed.hasNext()) {
            final LogSegment curr = unflushed.next();
            LOG.info(String.format("Recovering unflushed segment %d in log %s.", curr.getBaseOffset(), name()));
            int truncatedBytes = 0;
            try {
                truncatedBytes = curr.recover(config.getMaxMessageSize());
                ;
            } catch (InvalidOffsetException e) {
                long startOffset = curr.getBaseOffset();
                LOG.warn("Found invalid offset during recovery for log " + dir.getName() + ". Deleting the corrupt segment and " +
                        "creating an empty one with starting offset " + startOffset);
                curr.truncateTo(startOffset);
            }
            if (truncatedBytes > 0) {
                // we had an invalid message, delete all remaining log
                LOG.warn(String.format("Corruption found in segment %d of log %s, truncating to offset %d.", curr.getBaseOffset(), name(), curr.nextOffset()));
                while (unflushed.hasNext()) {
                    final LogSegment logSegment = unflushed.next();
                    try {
                        deleteSegment(logSegment);
                    } catch (SamsaStorageException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * Check if we have the "clean shutdown" file
     */
    private boolean hasCleanShutdownFile() {
        return new File(dir.getParentFile(), CLEAN_SHUTDOWN_FILE).exists();
    }

    /**
     * The number of segments in the log.
     * Take care! this is an O(n) operation.
     */
    public int numberOfSegments() {
        return segments.size();
    }

    /**
     * Close this log
     */
    public void close() {
        LOG.debug("Closing log " + name());
        synchronized (lock) {
            for (LogSegment logSegment : logSegments()) {
                logSegment.close();
            }
        }
    }

    public LogAppendInfo append(final ByteBufferMessageSet messages) throws SamsaStorageException, MessageSetSizeTooLargeException, MessageSizeTooLargeException, InvalidMessageSizeException, IOException {
        return append(messages, true);
    }

    /**
     * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
     * <p/>
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param messages      The message set to append
     * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
     * @return Information about the appended messages including the first and last offset.
     * @throws SamsaStorageException If the append fails due to an I/O error.
     */
    public LogAppendInfo append(final ByteBufferMessageSet messages, final boolean assignOffsets) throws MessageSizeTooLargeException, InvalidMessageSizeException, IOException, MessageSetSizeTooLargeException, SamsaStorageException {
        final LogAppendInfo appendInfo = analyzeAndValidateMessageSet(messages);

        // if we have any valid messages, append them to the log
        if (appendInfo.shallowCount == 0) {
            return appendInfo;
        }

        // trim any invalid bytes or partial messages before appending it to the on-disk log
        ByteBufferMessageSet validMessages = trimInvalidBytes(messages, appendInfo);

        try {
            // they are valid, insert them in the log
            synchronized (lock) {
                appendInfo.firstOffset = nextOffsetMetadata.getMessageOffset();

                if (assignOffsets) {
                    // assign offsets to the message set
                    final AtomicLong offset = new AtomicLong(nextOffsetMetadata.getMessageOffset());
                    try {
                        validMessages = validMessages.assignOffsets(offset, appendInfo.codec);
                    } catch (IOException e) {
                        throw new SamsaException(String.format("Error in validating messages while appending to log '%s'", name()), e);
                    }
                    appendInfo.lastOffset = offset.get() - 1;
                } else {
                    // we are taking the offsets we are given
                    if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.getMessageOffset()) {
                        throw new IllegalArgumentException("Out of order offsets found in " + messages);
                    }
                }

                // re-validate message sizes since after re-compression some may exceed the limit
                final Iterator<MessageAndOffset> messageAndOffsetIterator = validMessages.shallowIterator();
                while (messageAndOffsetIterator.hasNext()) {
                    final MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
                    if (MessageSet.entrySize(messageAndOffset.getMessage()) > config.getMaxMessageSize()) {
                        // we record the original message set size instead of trimmed size
                        // to be consistent with pre-compression bytesRejectedRate recording
                        // TODO BrokerTopicStats
                        //BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
                        //BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
                        throw new MessageSizeTooLargeException(String.format("Message size is %d bytes which exceeds the maximum configured message size of %d.",
                                MessageSet.entrySize(messageAndOffset.getMessage()), config.getMaxMessageSize()));
                    }
                }

                // check messages set size may be exceed config.segmentSize
                if (validMessages.sizeInBytes() > config.getSegmentSize()) {
                    throw new MessageSetSizeTooLargeException(String.format("Message set size is %d bytes which exceeds the maximum configured segment size of %d.",
                            validMessages.sizeInBytes(), config.getSegmentSize()));
                }


                // maybe roll the log if this segment is full
                final LogSegment segment = maybeRoll(validMessages.sizeInBytes());

                // now append to the log
                segment.append(appendInfo.firstOffset, validMessages);

                // increment the log end offset
                updateLogEndOffset(appendInfo.lastOffset + 1);

                LOG.trace(String.format("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s",
                        name(), appendInfo.firstOffset, nextOffsetMetadata.getMessageOffset(), validMessages));

                if (unflushedMessages() >= config.getFlushInterval()) {
                    flush();
                }

                return appendInfo;
            }
        } catch (IOException e) {
            throw new SamsaStorageException(String.format("I/O exception in append to log '%s'", name()), e);
        }
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid
     * </ol>
     * <p/>
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private LogAppendInfo analyzeAndValidateMessageSet(final ByteBufferMessageSet messages) throws MessageSizeTooLargeException {
        int shallowMessageCount = 0;
        int validBytesCount = 0;
        long firstOffset = -1L;
        long lastOffset = -1L;
        CompressionCodec codec = CompressionCodec.NONE;
        boolean monotonic = true;
        final Iterator<MessageAndOffset> messageAndOffsetIterator = messages.shallowIterator();
        while (messageAndOffsetIterator.hasNext()) {
            final MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
            // update the first offset if on the first message
            if (firstOffset < 0) {
                firstOffset = messageAndOffset.getOffset();
            }
            // check that offsets are monotonically increasing
            if (lastOffset >= messageAndOffset.getOffset()) {
                monotonic = false;
            }
            // update the last offset seen
            lastOffset = messageAndOffset.getOffset();

            final Message m = messageAndOffset.getMessage();

            // Check if the message sizes are valid.
            final int messageSize = MessageSet.entrySize(m);
            if (messageSize > config.getMaxMessageSize()) {
                //BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes)
                //BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(messages.sizeInBytes)
                throw new MessageSizeTooLargeException(String.format("Message size is %d bytes which exceeds the maximum configured message size of %d.",
                        messageSize, config.getMaxMessageSize()));
            }

            // check the validity of the message by checking CRC
            m.ensureValid();

            shallowMessageCount += 1;
            validBytesCount += messageSize;

            final CompressionCodec messageCodec = m.compressionCodec();
            if (messageCodec != CompressionCodec.NONE) {
                codec = messageCodec;
            }
        }
        return new LogAppendInfo(firstOffset, lastOffset, codec, shallowMessageCount, validBytesCount, monotonic);
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param messages The message set to trim
     * @param info     The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in or it may not.
     */
    private ByteBufferMessageSet trimInvalidBytes(final ByteBufferMessageSet messages, final LogAppendInfo info) throws InvalidMessageSizeException {
        final int messageSetValidBytes = info.validBytes;
        if (messageSetValidBytes < 0) {
            throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        }
        if (messageSetValidBytes == messages.sizeInBytes()) {
            return messages;
        } else {
            // trim invalid bytes
            final ByteBuffer validByteBuffer = messages.getBuffer().duplicate();
            validByteBuffer.limit(messageSetValidBytes);
            return new ByteBufferMessageSet(validByteBuffer);
        }
    }

    /**
     * Read messages from the log
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength   The maximum number of bytes to read
     * @param maxOffset   -The offset to read up to, exclusive. (i.e. the first offset NOT included in the resulting message set).
     * @return The fetch data information including fetch starting offset metadata and messages read
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
     */
    public FetchDataInfo read(final long startOffset, final int maxLength, final Optional<Long> maxOffset) throws OffsetOutOfRangeException, IOException {
        LOG.trace(String.format("Reading %d bytes from offset %d in log %s of length %d bytes", maxLength, startOffset, name(), size()));

        // check if the offset is valid and in range
        final long next = nextOffsetMetadata.getMessageOffset();
        if (startOffset == next) {
            return new FetchDataInfo(nextOffsetMetadata, MessageSet.EMPTY);
        }

        Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);

        // attempt to read beyond the log end offset is an error
        if (startOffset > next || entry == null) {
            throw new OffsetOutOfRangeException(String.format("Request for offset %d but we only have log segments in the range %d to %d.", startOffset, segments.firstKey(), next));
        }

        // do the read on the segment with a base offset less than the target offset
        // but if that segment doesn't contain any messages with an offset greater than that
        // continue to read from successive segments until we get some messages or we reach the end of the log
        while (entry != null) {
            final FetchDataInfo fetchInfo = entry.getValue().read(startOffset, maxOffset, maxLength);

            if (fetchInfo == null) {
                entry = segments.higherEntry(entry.getKey());
            } else {
                return fetchInfo;
            }
        }

        // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
        // this can happen when all messages with offset larger than start offsets have been deleted.
        // In this case, we will return the empty set with log end offset metadata
        return new FetchDataInfo(nextOffsetMetadata, MessageSet.EMPTY);
    }

    public FetchDataInfo read(final long startOffset, final int maxLength) throws OffsetOutOfRangeException, IOException {
        return read(startOffset, maxLength, Optional.<Long>absent());
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * If the message offset is out of range, return unknown offset metadata
     */
    public LogOffsetMetadata convertToOffsetMetadata(final long offset) throws IOException, OffsetOutOfRangeException {
        try {
            final FetchDataInfo fetchDataInfo = read(offset, 1, Optional.<Long>absent());
            return fetchDataInfo.getOffsetMetadata();
        } catch (OffsetOutOfRangeException e) {
            return LogOffsetMetadata.UNKNOWN_OFFSET_METADATA;
        }
    }

    /**
     * Delete any log segments matching the given predicate function,
     * starting with the oldest segment and moving forward until a segment doesn't match.
     *
     * @param predicate A function that takes in a single log segment and returns true iff it is deletable
     * @return The number of segments deleted
     */
    public int deleteOldSegments(final Function<LogSegment, Boolean> predicate) throws IOException, SamsaStorageException {
        // find any segments that match the user-supplied predicate UNLESS it is the final segment
        // and it is empty (since we would just end up re-creating it
        final LogSegment lastSegment = activeSegment();

        List<LogSegment> deletable = Lists.newArrayList();
        for (LogSegment segment : logSegments()) {
            if (predicate.apply(segment) && (segment.getBaseOffset() != lastSegment.getBaseOffset() || segment.size() > 0)) {
                deletable.add(segment);
            }
        }

        int numToDelete = deletable.size();
        if (numToDelete > 0) {
            synchronized (lock) {
                // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
                if (segments.size() == numToDelete) {
                    roll();
                }
                // remove the segments for lookups
                for (LogSegment segment : deletable) {
                    deleteSegment(segment);
                }
            }
        }
        return numToDelete;
    }

    /**
     * The size of the log in bytes
     */
    public long size() {
        long size = 0;
        for (LogSegment segment : logSegments()) {
            size += segment.size();
        }

        return size;
    }

    /**
     * The earliest message offset in the log
     */
    public long logStartOffset() {
        return logSegments().iterator().next().getBaseOffset();
    }

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    public LogOffsetMetadata logEndOffsetMetadata() {
        return nextOffsetMetadata;
    }

    /**
     * The offset of the next message that will be appended to the log
     */
    public long logEndOffset() {
        return nextOffsetMetadata.getMessageOffset();
    }

    /**
     * Roll the log over to a new empty log segment if necessary.
     *
     * @param messagesSize The messages set size in bytes
     *                     logSegment will be rolled if one of the following conditions met
     *                     <ol>
     *                     <li> The logSegment is full
     *                     <li> The maxTime has elapsed
     *                     <li> The index is full
     *                     </ol>
     * @return The currently active segment after (perhaps) rolling to a new segment
     */
    private LogSegment maybeRoll(final int messagesSize) throws IOException {
        final LogSegment segment = activeSegment();
        if (segment.size() > config.getSegmentSize() - messagesSize ||
                segment.size() > 0 && time.milliseconds() - segment.getCreated() > config.getSegmentMs() - segment.getRollJitterMs() ||
                segment.getIndex().isFull()) {
            LOG.debug(String.format("Rolling new log segment in %s (log_size = %d/%d, index_size = %d/%d, age_ms = %d/%d).",
                    name(),
                    segment.size(),
                    config.getSegmentSize(),
                    segment.getIndex().entries(),
                    segment.getIndex().getMaxEntries(),
                    time.milliseconds() - segment.getCreated(),
                    config.getSegmentMs() - segment.getRollJitterMs()));
            return roll();
        } else {
            return segment;
        }
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *
     * @return The newly rolled segment
     */
    public LogSegment roll() throws IOException {
        long start = time.nanoseconds();

        synchronized (lock) {
            final long newOffset = logEndOffset();
            final File logFile = logFilename(dir, newOffset);
            final File indexFile = indexFilename(dir, newOffset);
            for (final File file : Lists.newArrayList(logFile, indexFile)) {
                if (!file.exists()) {
                    continue;
                }
                LOG.warn("Newly rolled segment file " + file.getName() + " already exists; deleting it first");
                file.delete();
            }

            final Map.Entry<Long, LogSegment> lastEntry = segments.lastEntry();
            if (lastEntry != null) {
                lastEntry.getValue().getIndex().trimToValidSize();
            }
            final LogSegment segment = new LogSegment(dir,
                    newOffset,
                    config.getIndexInterval(),
                    config.getMaxIndexSize(),
                    config.getRandomSegmentJitter(),
                    time);
            final LogSegment prev = addSegment(segment);
            if (prev != null) {
                throw new SamsaException(String.format("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.", name(), newOffset));
            }

            // schedule an asynchronous flush of the old segment
            scheduler.schedule("flush-log", new Runnable() {
                @Override
                public void run() {
                    try {
                        flush(newOffset);
                    } catch (IOException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }, 0L, -1, TimeUnit.MILLISECONDS);

            LOG.info("Rolled new log segment for '" + name() + String.format("' in %.0f ms.", (time.nanoseconds() - start) / (1000.0 * 1000.0)));

            return segment;
        }
    }

    /**
     * The number of messages appended to the log since the last flush
     */
    public long unflushedMessages() {
        return logEndOffset() - recoveryPoint;
    }

    /**
     * Flush all log segments
     */
    public void flush() throws IOException {
        flush(logEndOffset());
    }

    /**
     * Flush log segments for all offsets up to offset-1
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    public void flush(final long offset) throws IOException {
        if (offset <= recoveryPoint) {
            return;
        }
        LOG.debug("Flushing log '" + name() + " up to offset " + offset + ", last flushed: " + lastFlushTime() + " current time: " +
                time.milliseconds() + " unflushed = " + unflushedMessages());
        for (final LogSegment segment : logSegments(recoveryPoint, offset)) {
            segment.flush();
        }
        synchronized (lock) {
            if (offset > recoveryPoint) {
                recoveryPoint = offset;
                lastflushedTime.set(time.milliseconds());
            }
        }
    }

    /**
     * Completely delete this log directory and all contents from the file system with no delay
     */
    public void delete() throws SamsaStorageException {
        synchronized (lock) {
            for (LogSegment segment : logSegments()) {
                segment.delete();
            }

            segments.clear();
            Utils.rm(dir);
        }
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     */
    public void truncateTo(final long targetOffset) throws IOException, SamsaStorageException {
        LOG.info(String.format("Truncating log %s to offset %d.", name(), targetOffset));
        if (targetOffset < 0) {
            throw new IllegalArgumentException(String.format("Cannot truncate to a negative offset (%d).", targetOffset));
        }
        if (targetOffset > logEndOffset()) {
            LOG.info(String.format("Truncating %s to %d has no effect as the largest offset in the log is %d.", name(), targetOffset, logEndOffset() - 1));
            return;
        }
        synchronized (lock) {
            if (segments.firstEntry().getValue().getBaseOffset() > targetOffset) {
                truncateFullyAndStartAt(targetOffset);
            } else {
                for (LogSegment segment : logSegments()) {
                    if (segment.getBaseOffset() > targetOffset) {
                        deleteSegment(segment);
                    }
                }
                activeSegment().truncateTo(targetOffset);
                updateLogEndOffset(targetOffset);
                recoveryPoint = Math.min(targetOffset, recoveryPoint);
            }
        }
    }

    /**
     * Delete all data in the log and start at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    public void truncateFullyAndStartAt(final long newOffset) throws SamsaStorageException, IOException {
        LOG.debug("Truncate and start log '" + name() + "' to " + newOffset);
        synchronized (lock) {
            final ArrayList<LogSegment> segmentsToDelete = Lists.newArrayList(logSegments());
            for (LogSegment segment : segmentsToDelete) {
                deleteSegment(segment);
            }

            addSegment(new LogSegment(dir,
                    newOffset,
                    config.getIndexInterval(),
                    config.getMaxIndexSize(),
                    config.getRandomSegmentJitter(),
                    time));
            updateLogEndOffset(newOffset);
            recoveryPoint = Math.min(newOffset, recoveryPoint);
        }
    }

    /**
     * The time this log is last known to have been fully flushed to disk
     */
    public long lastFlushTime() {
        return lastflushedTime.get();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return segments.lastEntry().getValue();
    }

    /**
     * All the log segments in this log ordered from oldest to newest
     */
    public Iterable<LogSegment> logSegments() {
        return segments.values();
    }

    /**
     * Get all segments beginning with the segment that includes "from" and ending with the segment
     * that includes up to "to-1" or the end of the log (if to > logEndOffset)
     */
    public Iterable<LogSegment> logSegments(final long from, final long to) {
        synchronized (lock) {
            final Long floor = segments.floorKey(from);
            if (floor == null) {
                return segments.headMap(to).values();
            } else {
                return segments.subMap(floor, true, to, false).values();
            }
        }
    }

    @Override
    public String toString() {
        return "Log(" + dir + ")";
    }


    /**
     * This method performs an asynchronous log segment delete by doing the following:
     * <ol>
     * <li>It removes the segment from the segment map so that it will no longer be used for reads.
     * <li>It renames the index and log files by appending .deleted to the respective file name
     * <li>It schedules an asynchronous delete operation to occur in the future
     * </ol>
     * This allows reads to happen concurrently without synchronization and without the possibility of physically
     * deleting a file while it is being read from.
     *
     * @param segment The log segment to schedule for deletion
     */
    private void deleteSegment(final LogSegment segment) throws SamsaStorageException {
        LOG.info(String.format("Scheduling log segment %d for log %s for deletion.", segment.getBaseOffset(), name()));
        synchronized (lock) {
            segments.remove(segment.getBaseOffset());
            asyncDeleteSegment(segment);
        }
    }

    /**
     * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
     *
     * @throws SamsaStorageException if the file can't be renamed and still exists
     */
    private void asyncDeleteSegment(final LogSegment segment) throws SamsaStorageException {
        segment.changeFileSuffixes("", Log.DELETED_FILE_SUFFIX);
        scheduler.schedule("delete-file", new Runnable() {
            @Override
            public void run() {
                LOG.info(String.format("Deleting segment %d from log %s.", segment.getBaseOffset(), name()));
                try {
                    segment.delete();
                } catch (SamsaStorageException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, config.getFileDeleteDelayMs(), -1, TimeUnit.MILLISECONDS);
    }

    /**
     * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
     * be asynchronously deleted.
     *
     * @param newSegment  The new log segment to add to the log
     * @param oldSegments The old log segments to delete from the log
     */
    public void replaceSegments(final LogSegment newSegment, final List<LogSegment> oldSegments) throws SamsaStorageException {
        synchronized (lock) {
            // need to do this in two phases to be crash safe AND do the delete asynchronously
            // if we crash in the middle of this we complete the swap in loadSegments()
            newSegment.changeFileSuffixes(Log.CLEANED_FILE_SUFFIX, Log.SWAP_FILE_SUFFIX);
            addSegment(newSegment);

            // delete the old files
            for (final LogSegment seg : oldSegments) {
                // remove the index entry
                if (seg.getBaseOffset() != newSegment.getBaseOffset()) {
                    segments.remove(seg.getBaseOffset());
                }
                // delete segment
                asyncDeleteSegment(seg);
            }
            // okay we are safe now, remove the swap suffix
            newSegment.changeFileSuffixes(Log.SWAP_FILE_SUFFIX, "");
        }
    }

    /**
     * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
     *
     * @param segment The segment to add
     */
    public LogSegment addSegment(final LogSegment segment) {
        return segments.put(segment.getBaseOffset(), segment);
    }

    public File getDir() {
        return dir;
    }

    public LogConfig getConfig() {
        return config;
    }

    public long getRecoveryPoint() {
        return recoveryPoint;
    }

    public TopicAndPartition getTopicAndPartition() {
        return topicAndPartition;
    }
}
