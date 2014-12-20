package com.github.bernd.samsa;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.FileMessageSet;
import com.github.bernd.samsa.message.InvalidMessageException;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.utils.SamsaTime;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * <p/>
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 * <p/>
 * NOT thread-safe!
 */
public class LogSegment {
    private static final Logger LOG = LoggerFactory.getLogger(LogSegment.class);

    private final FileMessageSet log;
    private final OffsetIndex index;
    private final long baseOffset;
    private final int indexIntervalBytes;
    private final long rollJitterMs;
    private final SamsaTime time;
    private long created;

    /* the number of bytes since we last added an entry in the offset index */
    private long bytesSinceLastIndexEntry = 0;

    /**
     * @param log                The message set containing log entries
     * @param index              The offset index
     * @param baseOffset         A lower bound on the offsets in this segment
     * @param indexIntervalBytes The approximate number of bytes between entries in the index
     * @param time               The time instance
     */
    public LogSegment(final FileMessageSet log,
                      final OffsetIndex index,
                      final long baseOffset,
                      final int indexIntervalBytes,
                      final long rollJitterMs,
                      final SamsaTime time) {
        this.log = log;
        this.index = index;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.rollJitterMs = rollJitterMs;
        this.time = time;
        created = time.milliseconds();
    }

    public LogSegment(final File dir,
                      final long startOffset,
                      final int indexIntervalBytes,
                      final int maxIndexSize,
                      final long rollJitterMs,
                      final SamsaTime time) throws IOException {
        this(new FileMessageSet(Log.logFilename(dir, startOffset)),
                new OffsetIndex(Log.indexFilename(dir, startOffset), startOffset, maxIndexSize),
                startOffset,
                indexIntervalBytes,
                rollJitterMs,
                time);
    }

    /**
     * Return the size in bytes of this log segment
     */
    public long size() {
        return log.sizeInBytes();
    }

    /**
     * Append the given messages starting with the given offset. Add
     * an entry to the index if needed.
     * <p/>
     * It is assumed this method is being called from within a lock.
     * <p/>
     * NOT thread-safe!
     *
     * @param offset   The first offset in the message set.
     * @param messages The messages to append.
     */
    public void append(final long offset, final ByteBufferMessageSet messages) {
        if (messages.sizeInBytes() > 0) {
            LOG.trace(String.format("Inserting %d bytes at offset %d at position %d", messages.sizeInBytes(), offset, log.sizeInBytes()));
            // append an entry to the index (if needed)
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                index.append(offset, log.sizeInBytes());
                bytesSinceLastIndexEntry = 0;
            }
            // append the messages
            log.append(messages);
            bytesSinceLastIndexEntry += messages.sizeInBytes();
        }
    }

    /**
     * Find the physical file position for the first message with offset >= the requested offset.
     * <p/>
     * The lowerBound argument is an optimization that can be used if we already know a valid starting position
     * in the file higher than the greatest-lower-bound from the index.
     *
     * @param offset               The offset we want to translate
     * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
     *                             when omitted, the search will begin at the position in the offset index.
     *                             <p/>
     *                             Marked as @threadsafe
     * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
     */
    private OffsetPosition translateOffset(final long offset, final int startingFilePosition) throws IOException {
        final OffsetPosition mapping = index.lookup(offset);
        return log.searchFor(offset, Math.max(mapping.getPosition(), startingFilePosition));
    }

    private OffsetPosition translateOffset(final long offset) throws IOException {
        return translateOffset(offset, 0);
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
     * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     * <p/>
     * Marked as @threadsafe
     *
     * @param startOffset A lower bound on the first offset to include in the message set we read
     * @param maxSize     The maximum number of bytes to include in the message set we read
     * @param maxOffset   An optional maximum offset for the message set we read
     * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
     * or null if the startOffset is larger than the largest offset in this log
     */
    public FetchDataInfo read(final long startOffset, final Optional<Long> maxOffset, final int maxSize) throws IOException {
        if (maxSize < 0) {
            throw new IllegalArgumentException(String.format("Invalid max size for log read (%d)", maxSize));
        }

        int logSize = log.sizeInBytes(); // this may change, need to save a consistent copy
        final OffsetPosition startPosition = translateOffset(startOffset);

        // if the start position is already off the end of the log, return null
        if (startPosition == null) {
            return null;
        }

        final LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(startOffset, baseOffset, startPosition.getPosition());

        // if the size is zero, still return a log segment but with zero size
        if (maxSize == 0) {
            return new FetchDataInfo(offsetMetadata, MessageSet.EMPTY);
        }

        // calculate the length of the message set to read based on whether or not they gave us a maxOffset
        int length;
        if (maxOffset.isPresent()) {
            // no max offset, just use the max size they gave unmolested
            length = maxSize;
        } else {
            long offset = maxOffset.get();

            // there is a max offset, translate it to a file position and use that to calculate the max read size
            if (offset < startOffset) {
                throw new IllegalArgumentException(String.format("Attempt to read with a maximum offset (%d) less than the start offset (%d).", offset, startOffset));
            }
            OffsetPosition mapping = translateOffset(offset, startPosition.getPosition());
            int endPosition;
            if (mapping == null) {
                endPosition = logSize; // the max offset is off the end of the log, use the end of the file
            } else {
                endPosition = mapping.getPosition();
            }

            length = Math.min(endPosition - startPosition.getPosition(), maxSize);

        }
        return new FetchDataInfo(offsetMetadata, log.read(startPosition.getPosition(), length));
    }

    /**
     * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
     * <p/>
     * Marked as @nonthreadsafe
     *
     * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
     *                       is corrupt.
     * @return The number of bytes truncated from the log
     */
    public int recover(final int maxMessageSize) throws IOException {
        index.truncate();
        index.resize(index.getMaxIndexSize());
        int validBytes = 0;
        int lastIndexEntry = 0;
        final Iterator<MessageAndOffset> iter = log.iterator(maxMessageSize);
        try {
            while (iter.hasNext()) {
                final MessageAndOffset entry = iter.next();
                entry.getMessage().ensureValid();
                if (validBytes - lastIndexEntry > indexIntervalBytes) {
                    // we need to decompress the message, if required, to get the offset of the first uncompressed message
                    long startOffset;
                    if (entry.getMessage().compressionCodec() == CompressionCodec.NONE) {
                        startOffset = entry.getOffset();
                    } else {
                        startOffset = ByteBufferMessageSet.decompress(entry.getMessage()).iterator().next().getOffset();
                    }
                    index.append(startOffset, validBytes);
                    lastIndexEntry = validBytes;
                }
                validBytes += MessageSet.entrySize(entry.getMessage());
            }
        } catch (InvalidMessageException e) {
            LOG.warn(String.format("Found invalid messages in log segment %s at byte offset %d: %s.", log.getFile().getAbsolutePath(), validBytes, e.getMessage()));
        }
        int truncated = log.sizeInBytes() - validBytes;
        log.truncateTo(validBytes);
        index.trimToValidSize();
        return truncated;
    }

    @Override
    public String toString() {
        return "LogSegment(baseOffset=" + baseOffset + ", size=" + size() + ")";
    }

    /**
     * Truncate off all index and log entries with offsets >= the given offset.
     * If the given offset is larger than the largest message in this segment, do nothing.
     * <p/>
     * Marked as @nonthreadsafe
     *
     * @param offset The offset to truncate to
     * @return The number of log bytes truncated
     */
    public int truncateTo(final long offset) throws IOException {
        final OffsetPosition mapping = translateOffset(offset);
        if (mapping == null) {
            return 0;
        }
        index.truncateTo(offset);
        // after truncation, reset and allocate more space for the (new currently  active) index
        index.resize(index.getMaxIndexSize());
        int bytesTruncated = log.truncateTo(mapping.getPosition());
        if (log.sizeInBytes() == 0) {
            created = time.milliseconds();
        }
        bytesSinceLastIndexEntry = 0;
        return bytesTruncated;
    }

    /**
     * Calculate the offset that would be used for the next message to be append to this segment.
     * Note that this is expensive.
     * <p/>
     * Marked as @threadsafe
     */
    public long nextOffset() throws IOException {
        FetchDataInfo ms = read(index.getLastOffset(), Optional.<Long>absent(), log.sizeInBytes());
        if (ms == null) {
            return baseOffset;
        } else {
            final MessageSet messageSet = ms.getMessageSet();
            if (Iterables.isEmpty(messageSet)) {
                return baseOffset;
            } else {
                return Iterables.getLast(messageSet).nextOffset();
            }
        }
    }

    /**
     * Flush this log segment to disk
     * Marked as @threadsafe
     */
    public void flush() throws IOException {
        log.flush();
        index.flush();
    }

    /**
     * Change the suffix for the index and log file for this log segment
     */
    public void changeFileSuffixes(final String oldSuffix, final String newSuffix) throws SamsaStorageException {
        final boolean logRenamed = log.renameTo(new File(Utils.replaceSuffix(log.getFile().getPath(), oldSuffix, newSuffix)));
        if (!logRenamed) {
            throw new SamsaStorageException(String.format("Failed to change the log file suffix from %s to %s for log segment %d", oldSuffix, newSuffix, baseOffset));
        }
        final boolean indexRenamed = index.renameTo(new File(Utils.replaceSuffix(index.getFile().getPath(), oldSuffix, newSuffix)));
        if (!indexRenamed) {
            throw new SamsaStorageException(String.format("Failed to change the index file suffix from %s to %s for log segment %d", oldSuffix, newSuffix, baseOffset));
        }
    }

    /**
     * Close this log segment
     */
    public void close() {
        try {
            // Utils.swallow(index.close)
            // Utils.swallow(log.close)
            index.close();
            log.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * Delete this log segment from the filesystem.
     *
     * @throws SamsaStorageException if the delete fails.
     */
    public void delete() throws SamsaStorageException {
        final boolean deletedLog = log.delete();
        final boolean deletedIndex = index.delete();
        if (!deletedLog && log.getFile().exists()) {
            throw new SamsaStorageException("Delete of log " + log.getFile().getName() + " failed.");
        }
        if (!deletedIndex && index.getFile().exists()) {
            throw new SamsaStorageException("Delete of index " + index.getFile().getName() + " failed.");
        }
    }

    /**
     * The last modified time of this log segment as a unix time stamp
     */
    public long lastModified() {
        return log.getFile().lastModified();
    }

    /**
     * Change the last modified time for this log segment
     */
    public void setLastModified(final long ms) {
        log.getFile().setLastModified(ms);
        index.getFile().setLastModified(ms);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public FileMessageSet getLog() {
        return log;
    }

    public OffsetIndex getIndex() {
        return index;
    }

    public long getCreated() {
        return created;
    }

    public long getRollJitterMs() {
        return rollJitterMs;
    }

    public int getIndexIntervalBytes() {
        return indexIntervalBytes;
    }
}
