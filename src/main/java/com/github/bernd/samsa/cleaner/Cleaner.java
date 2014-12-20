package com.github.bernd.samsa.cleaner;

import com.github.bernd.samsa.Log;
import com.github.bernd.samsa.LogSegment;
import com.github.bernd.samsa.OffsetIndex;
import com.github.bernd.samsa.OffsetMap;
import com.github.bernd.samsa.OffsetMapException;
import com.github.bernd.samsa.SamsaStorageException;
import com.github.bernd.samsa.TopicAndPartition;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.FileMessageSet;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.utils.Time;
import com.github.bernd.samsa.utils.Throttler;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

/**
 * This class holds the actual logic for cleaning a log
 * <p/>
 * This class it not thread-safe!
 */
public class Cleaner {
    private static final Logger LOG = LoggerFactory.getLogger(Cleaner.class);

    /* cleaning stats - one instance for the current (or next) cleaning cycle and one for the last completed cycle */
    private CleanerStats[] statsUnderlying;
    private CleanerStats stats;

    /* buffer used for read i/o */
    private ByteBuffer readBuffer;

    /* buffer used for write i/o */
    private ByteBuffer writeBuffer;

    private final int id;
    private final OffsetMap offsetMap;
    private final int ioBufferSize;
    private final int maxIoBufferSize;
    private final double dupBufferLoadFactor;
    private final Throttler throttler;
    private final Time time;
    private final CheckDoneCallback<TopicAndPartition> checkDone;

    /**
     * @param id              An identifier used for logging
     * @param offsetMap       The map used for deduplication
     * @param maxIoBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
     * @param throttler       The throttler instance to use for limiting I/O rate.
     * @param time            The time instance
     */
    public Cleaner(final int id,
                   final OffsetMap offsetMap,
                   final int ioBufferSize,
                   final int maxIoBufferSize,
                   final double dupBufferLoadFactor,
                   final Throttler throttler,
                   final Time time,
                   final CheckDoneCallback<TopicAndPartition> checkDone) {
        this.id = id;
        this.offsetMap = offsetMap;
        this.ioBufferSize = ioBufferSize;
        this.maxIoBufferSize = maxIoBufferSize;
        this.dupBufferLoadFactor = dupBufferLoadFactor;
        this.throttler = throttler;
        this.time = time;
        this.checkDone = checkDone;
        this.statsUnderlying = new CleanerStats[]{new CleanerStats(time), new CleanerStats(time)};
        this.stats = statsUnderlying[0];

        this.readBuffer = ByteBuffer.allocate(ioBufferSize);
        this.writeBuffer = ByteBuffer.allocate(ioBufferSize);
    }

    /**
     * Clean the given log
     *
     * @param cleanable The log to be cleaned
     * @return The first offset not cleaned
     */
    public long clean(final LogToClean cleanable) throws IOException, OffsetMapException, SamsaStorageException, LogCleaningAbortedException {
        stats.clear();
        LOG.info(String.format("Beginning cleaning of log %s.", cleanable.getLog().name()));
        final Log log = cleanable.getLog();

        // build the offset map
        LOG.info(String.format("Building offset map for %s...", cleanable.getLog().name()));
        final long upperBoundOffset = log.activeSegment().getBaseOffset();
        final long endOffset = buildOffsetMap(log, cleanable.getFirstDirtyOffset(), upperBoundOffset, offsetMap) + 1;
        stats.indexDone();

        // figure out the timestamp below which it is safe to remove delete tombstones
        // this position is defined to be a configurable time beneath the last modified time of the last clean segment
        final long deleteHorizonMs;

        final Iterable<LogSegment> segments = log.logSegments(0, cleanable.getFirstDirtyOffset());
        if (Iterables.isEmpty(segments)) {
            deleteHorizonMs = 0L;
        } else {
            deleteHorizonMs = Iterables.getLast(segments).lastModified() - log.getConfig().getDeleteRetentionMs();
        }

        // group the segments and clean the groups
        LOG.info(String.format("Cleaning log %s (discarding tombstones prior to %s)...", log.name(), new Date(deleteHorizonMs)));
        for (final List<LogSegment> group : groupSegmentsBySize(log.logSegments(0, endOffset), log.getConfig().getSegmentSize(), log.getConfig().getMaxIndexSize())) {
            cleanSegments(log, group, offsetMap, deleteHorizonMs);
        }

        // record buffer utilization
        stats.bufferUtilization = offsetMap.getUtilization();

        stats.allDone();

        return endOffset;
    }

    /**
     * Clean a group of segments into a single replacement segment
     *
     * @param log             The log being cleaned
     * @param segments        The group of segments being cleaned
     * @param map             The offset map to use for cleaning segments
     * @param deleteHorizonMs The time to retain delete tombstones
     */
    public void cleanSegments(final Log log,
                              final List<LogSegment> segments,
                              final OffsetMap map,
                              final long deleteHorizonMs) throws IOException, OffsetMapException, SamsaStorageException, LogCleaningAbortedException {
        // create a new segment with the suffix .cleaned appended to both the log and index name
        final File logFile = new File(segments.get(0).getLog().getFile().getPath() + Log.CLEANED_FILE_SUFFIX);
        logFile.delete();
        final File indexFile = new File(segments.get(0).getIndex().getFile().getPath() + Log.CLEANED_FILE_SUFFIX);
        indexFile.delete();
        final FileMessageSet messages = new FileMessageSet(logFile);
        final OffsetIndex index = new OffsetIndex(indexFile, segments.get(0).getBaseOffset(), segments.get(0).getIndex().getMaxIndexSize());
        final LogSegment cleaned = new LogSegment(messages,index, segments.get(0).getBaseOffset(),
                segments.get(0).getIndexIntervalBytes(), log.getConfig().getSegmentJitterMs(), time);

        try {
            // clean segments into the new destination segment
            for (final LogSegment old : segments) {
                final boolean retainDeletes = old.lastModified() > deleteHorizonMs;
                LOG.info(String.format("Cleaning segment %s in log %s (last modified %s) into %s, %s deletes.",
                        old.getBaseOffset(), log.name(), new Date(old.lastModified()), cleaned.getBaseOffset(),
                        retainDeletes ? "retaining" : "discarding"));
                cleanInto(log.getTopicAndPartition(), old, cleaned, map, retainDeletes);
            }

            // trim excess index
            index.trimToValidSize();

            // flush new segment to disk before swap
            cleaned.flush();

            // update the modification date to retain the last modified date of the original files
            final long modified = Iterables.getLast(segments).lastModified();
            cleaned.setLastModified(modified);

            // swap in new segment
            final Iterable<String> baseOffsets = Iterables.transform(segments, new Function<LogSegment, String>() {
                @Override
                public String apply(LogSegment segment) {
                    return String.valueOf(segment.getBaseOffset());
                }
            });
            LOG.info(String.format("Swapping in cleaned segment %d for segment(s) %s in log %s.", cleaned.getBaseOffset(), Joiner.on(", ").join(baseOffsets), log.name()));
            log.replaceSegments(cleaned, segments);
        } catch (LogCleaningAbortedException e) {
            cleaned.delete();
            throw e;
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided
     *
     * @param source        The dirty log segment
     * @param dest          The cleaned log segment
     * @param map           The key=>offset mapping
     * @param retainDeletes Should delete tombstones be retained while cleaning this segment
     *                      <p/>
     *                      TODO: Implement proper compression support
     */
    private void cleanInto(final TopicAndPartition topicAndPartition,
                           final LogSegment source,
                           final LogSegment dest,
                           final OffsetMap map,
                           final boolean retainDeletes) throws IOException, OffsetMapException, LogCleaningAbortedException {
        int position = 0;
        while (position < source.getLog().sizeInBytes()) {
            checkDone.call(topicAndPartition);
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
            readBuffer.clear();
            writeBuffer.clear();
            final MessageSet messages = new ByteBufferMessageSet(source.getLog().readInto(readBuffer, position));
            throttler.maybeThrottle(messages.sizeInBytes());
            // check each message to see if it is to be retained
            int messagesRead = 0;
            for (final MessageAndOffset entry : messages) {
                messagesRead += 1;
                final int size = MessageSet.entrySize(entry.getMessage());
                position += size;
                stats.readMessage(size);
                final ByteBuffer key = entry.getMessage().key();
                Utils.require(key != null, "Found null key in log segment %s which is marked as dedupe.".format(source.getLog().getFile().getAbsolutePath()));
                long foundOffset = map.get(key);
               /* two cases in which we can get rid of a message:
                *   1) if there exists a message with the same key but higher offset
                *   2) if the message is a delete "tombstone" marker and enough time has passed
                */
                final boolean redundant = foundOffset >= 0 && entry.getOffset() < foundOffset;
                final boolean obsoleteDelete = !retainDeletes && entry.getMessage().isNull();
                if (!redundant && !obsoleteDelete) {
                    ByteBufferMessageSet.writeMessage(writeBuffer, entry.getMessage(), entry.getOffset());
                    stats.recopyMessage(size);
                }
            }
            // if any messages are to be retained, write them out
            if (writeBuffer.position() > 0) {
                writeBuffer.flip();
                final ByteBufferMessageSet retained = new ByteBufferMessageSet(writeBuffer);
                dest.append(Iterables.get(retained, 0).getOffset(), retained);
                throttler.maybeThrottle(writeBuffer.limit());
            }

            // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again
            if (readBuffer.limit() > 0 && messagesRead == 0) {
                growBuffers();
            }
        }
        restoreBuffers();
    }

    /**
     * Double the I/O buffer capacity
     */
    public void growBuffers() {
        if (readBuffer.capacity() >= maxIoBufferSize || writeBuffer.capacity() >= maxIoBufferSize) {
            throw new IllegalStateException(String.format("This log contains a message larger than maximum allowable size of %s.", maxIoBufferSize));
        }
        final int newSize = Math.min(readBuffer.capacity() * 2, maxIoBufferSize);
        LOG.info("Growing cleaner I/O buffers from " + readBuffer.capacity() + "bytes to " + newSize + " bytes.");
        readBuffer = ByteBuffer.allocate(newSize);
        writeBuffer = ByteBuffer.allocate(newSize);
    }

    /**
     * Restore the I/O buffer capacity to its original size
     */
    public void restoreBuffers() {
        if (readBuffer.capacity() > ioBufferSize) {
            readBuffer = ByteBuffer.allocate(ioBufferSize);
        }
        if (writeBuffer.capacity() > ioBufferSize) {
            writeBuffer = ByteBuffer.allocate(ioBufferSize);
        }
    }

    /**
     * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single
     * destination segment. This prevents segment sizes from shrinking too much.
     *
     * @param segments     The log segments to group
     * @param maxSize      the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     * @return A list of grouped segments
     */
    public List<List<LogSegment>> groupSegmentsBySize(final Iterable<LogSegment> segments,
                                                      final int maxSize,
                                                      final int maxIndexSize) {
        final List<List<LogSegment>> grouped = Lists.newArrayList();
        List<LogSegment> segs = Lists.newArrayList(segments);
        while (!segs.isEmpty()) {
            List<LogSegment> group = Lists.newArrayList(segs.get(0));
            long logSize = segs.get(0).size();
            int indexSize = segs.get(0).getIndex().sizeInBytes();
            segs = segs.subList(1, segs.size());
            while (!segs.isEmpty() &&
                    logSize + segs.get(0).size() < maxSize &&
                    indexSize + segs.get(0).getIndex().sizeInBytes() < maxIndexSize) {
                group.add(0, segs.get(0));
                logSize += segs.get(0).size();
                indexSize += segs.get(0).getIndex().sizeInBytes();
                segs = segs.subList(1, segs.size());
            }
            grouped.add(0, Lists.reverse(group));
        }
        return Lists.reverse(grouped);
    }

    /**
     * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
     *
     * @param log   The log to use
     * @param start The offset at which dirty messages begin
     * @param end   The ending offset for the map that is being built
     * @param map   The map in which to store the mappings
     * @return The final offset the map covers
     */
    public long buildOffsetMap(final Log log,
                               final long start,
                               final long end,
                               final OffsetMap map) throws IOException, OffsetMapException, LogCleaningAbortedException {
        map.clear();
        final List<LogSegment> dirty = Lists.newArrayList(log.logSegments(start, end));
        LOG.info(String.format("Building offset map for log %s for %d segments in offset range [%d, %d).", log.name(), dirty.size(), start, end));

        // Add all the dirty segments. We must take at least map.slots * load_factor,
        // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
        long offset = dirty.get(0).getBaseOffset();
        Utils.require(offset == start, String.format("Last clean offset is %d but segment base offset is %d for log %s.", start, offset, log.name()));
        final long minStopOffset = (long) (start + map.getSlots() * dupBufferLoadFactor);
        for (final LogSegment segment : dirty) {
            checkDone.call(log.getTopicAndPartition());
            if (segment.getBaseOffset() <= minStopOffset || map.getUtilization() < dupBufferLoadFactor) {
                offset = buildOffsetMapForSegment(log.getTopicAndPartition(), segment, map);
            }
        }
        LOG.info(String.format("Offset map for log %s complete.", log.name()));
        return offset;
    }

    /**
     * Add the messages in the given segment to the offset map
     *
     * @param segment The segment to index
     * @param map     The map in which to store the key=>offset mapping
     * @return The final offset covered by the map
     */
    private long buildOffsetMapForSegment(final TopicAndPartition topicAndPartition,
                                          final LogSegment segment,
                                          final OffsetMap map) throws IOException, OffsetMapException, LogCleaningAbortedException {
        int position = 0;
        long offset = segment.getBaseOffset();
        while (position < segment.getLog().sizeInBytes()) {
            checkDone.call(topicAndPartition);
            readBuffer.clear();
            final MessageSet messages = new ByteBufferMessageSet(segment.getLog().readInto(readBuffer, position));
            throttler.maybeThrottle(messages.sizeInBytes());
            int startPosition = position;
            for (final MessageAndOffset entry : messages) {
                final Message message = entry.getMessage();
                Utils.require(message.hasKey());
                final int size = MessageSet.entrySize(message);
                position += size;
                map.put(message.key(), entry.getOffset());
                offset = entry.getOffset();
                stats.indexMessage(size);
            }
            // if we didn't read even one complete message, our read buffer may be too small
            if (position == startPosition) {
                growBuffers();
            }
        }
        restoreBuffers();
        return offset;
    }

    public int getId() {
        return id;
    }

    public CleanerStats getStats() {
        return stats;
    }

    public void swapStats() {
        this.statsUnderlying = new CleanerStats[]{statsUnderlying[1], statsUnderlying[0]};
    }
}
