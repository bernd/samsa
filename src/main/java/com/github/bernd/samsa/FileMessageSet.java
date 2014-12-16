package com.github.bernd.samsa;

import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.MessageAndOffset;
import com.github.bernd.samsa.message.MessageSet;
import com.github.bernd.samsa.utils.IteratorTemplate;
import com.github.bernd.samsa.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An on-disk message set. An optional start and end position can be applied to the message set
 * which will allow slicing a subset of the file.
 *
 * This is NOT thread-safe!
 *
 */
public class FileMessageSet extends MessageSet {
    private static final Logger LOG = LoggerFactory.getLogger(FileMessageSet.class);

    private File file;
    private final FileChannel channel;
    private final int start;
    private final int end;

    /* the size of the message set in bytes */
    private final AtomicInteger _size;

    /**
     * @param file The file name for the underlying log data
     * @param channel the underlying file channel used
     * @param start A lower bound on the absolute position in the file from which the message set begins
     * @param end The upper bound on the absolute position in the file at which the message set ends
     * @param isSlice Should the start and end parameters be used for slicing?
     * @throws IOException
     */
    public FileMessageSet(final File file,
                          final FileChannel channel,
                          final int start,
                          final int end,
                          final boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;

        if (isSlice) {
            _size = new AtomicInteger(end - start);
        } else {
            _size = new AtomicInteger(Math.min((int) channel.size(), end) - start);
        }
        /* if this is not a slice, update the file pointer to the end of the file */
        if (!isSlice) {
            /* set the file position to the last byte in the file */
            channel.position(channel.size());
        }
    }

    /**
     * Create a file message set with no slicing.
     */
    public FileMessageSet(final File file, final FileChannel channel) throws IOException {
        this(file, channel, 0, Integer.MAX_VALUE, false);
    }

    /**
     * Create a file message set with no slicing
     */
    public FileMessageSet(final File file) throws IOException {
        this(file, Utils.openChannel(file, true));
    }

    /**
     * Create a file message set with mutable option
     */
    public FileMessageSet(final File file, final boolean mutable) throws IOException {
        this(file, Utils.openChannel(file, mutable));
    }

    /**
     * Create a slice view of the file message set that begins and ends at the given byte offsets
     */
    public FileMessageSet(final File file, final FileChannel channel, final int start, final int end) throws IOException {
        this(file, channel, start, end, true);
    }

    /**
     * Return a message set which is a view into this set starting from the given position and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     *
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileMessageSet read(final int position, final int size) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("Invalid position: " + position);
        }
        if (size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        return new FileMessageSet(file,
                channel,
                start + position,
                Math.min(start + position + size, sizeInBytes()));
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position. If no such offsets are found, return null.
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public OffsetPosition searchFor(final long targetOffset, final int startingPosition) throws IOException {
        int position = startingPosition;
        ByteBuffer buffer = ByteBuffer.allocate(MessageSet.LOG_OVERHEAD);
        int size = sizeInBytes();
        while ((position + MessageSet.LOG_OVERHEAD) < size) {
            buffer.rewind();
            channel.read(buffer, position);
            if (buffer.hasRemaining()) {
                throw new IllegalStateException(String.format(
                        "Failed to read complete buffer for targetOffset %d startPosition %d in %s",
                        targetOffset, startingPosition, file.getAbsolutePath()));
            }
            buffer.rewind();
            long offset = buffer.getLong();
            if (offset >= targetOffset) {
                return new OffsetPosition(offset, position);
            }
            int messageSize = buffer.getInt();
            if (messageSize < Message.MESSAGE_OVERHEAD) {
                throw new IllegalStateException("Invalid message size: " + messageSize);
            }
            position += MessageSet.LOG_OVERHEAD + messageSize;
        }
        return null;
    }

    /**
     * Write some of this set to the given channel.
     * @param destChannel The channel to write to.
     * @param writePosition The position in the message set to begin writing from.
     * @param size The maximum number of bytes to write
     * @return The number of bytes actually written.
     */
    @Override
    public int writeTo(final GatheringByteChannel destChannel, final long writePosition, final int size) throws IOException {
        // Ensure that the underlying size has not changed.
        int newSize = Math.min((int) channel.size(), end) - start;
        if (newSize < _size.get()) {
            throw new SamsaException(String.format(
                    "Size of FileMessageSet %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), _size.get(), newSize));
        }
        int bytesTransferred = (int) channel.transferTo(start + writePosition, Math.min(size, sizeInBytes()), destChannel);
        if (LOG.isTraceEnabled()) {
            LOG.trace("FileMessageSet {} : bytes transferred : {} bytes requested for transfer : {}",
                    file.getAbsolutePath(), bytesTransferred, Math.min(size, sizeInBytes()));
        }
        return bytesTransferred;
    }

    /**
     * Get a shallow iterator over the messages in the set.
     */
    @Override
    public Iterator<MessageAndOffset> iterator() {
        return iterator(Integer.MAX_VALUE);
    }

    /**
     * Get an iterator over the messages in the set. We only do shallow iteration here.
     * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory.
     * If we encounter a message larger than this we throw an InvalidMessageException.
     * @return The iterator.
     */
    public Iterator<MessageAndOffset> iterator(final int maxMessageSize) {
        return new IteratorTemplate<MessageAndOffset>() {
            int location = start;
            ByteBuffer sizeOffsetBuffer = ByteBuffer.allocate(12);

            @Override
            public MessageAndOffset makeNext() throws IOException {
                if (location >= end) {
                    return allDone();
                }

                // read the size of the item
                sizeOffsetBuffer.rewind();
                channel.read(sizeOffsetBuffer, location);

                if (sizeOffsetBuffer.hasRemaining()) {
                    return allDone();
                }

                sizeOffsetBuffer.rewind();
                long offset = sizeOffsetBuffer.getLong();
                int size = sizeOffsetBuffer.getInt();
                if (size < Message.MIN_HEADER_SIZE) {
                    return allDone();
                }
                if (size > maxMessageSize) {
                    throw new InvalidMessageException(String.format("Message size exceeds the largest allowable message size (%d).", maxMessageSize));
                }

                // read the item itself
                ByteBuffer buffer = ByteBuffer.allocate(size);
                channel.read(buffer, location + 12);
                if (buffer.hasRemaining()) {
                    return allDone();
                }
                buffer.rewind();

                // increment the location and return the item
                location += size + 12;
                return new MessageAndOffset(new Message(buffer.array()), offset);
            }
        };
    }

    /**
     * The number of bytes taken up by this file set
     */
    @Override
    public int sizeInBytes() {
        return _size.get();
    }

    /**
     * Append these messages to the message set
     */
    public void append(final ByteBufferMessageSet messages) {
        int written = messages.writeTo(channel, 0, messages.sizeInBytes());
        _size.getAndAdd(written);
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * Close this message set
     */
    public void close() throws IOException {
        flush();
        channel.close();
    }

    /**
     * Delete this message set from the filesystem
     * @return True iff this message set was deleted.
     */
    public boolean delete() {
        // Used to be the following in Kafka: Utils.swallow(channel.close());
        try {
            channel.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return file.delete();
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     * @param targetSize The size to truncate to.
     * @return The number of bytes truncated off
     */
    public int truncateTo(final int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0) {
            throw new SamsaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        }
        channel.truncate(targetSize);
        channel.position(targetSize);
        _size.set(targetSize);
        return originalSize - targetSize;
    }

    /**
     * Read from the underlying file into the buffer starting at the given position
     */
    public ByteBuffer readInto(final ByteBuffer buffer, final int relativePosition) throws IOException {
        channel.read(buffer, relativePosition + start);
        buffer.flip();
        return buffer;
    }

    /**
     * Rename the file that backs this message set
     * @return true iff the rename was successful
     */
    public boolean renameTo(final File f) {
        boolean success = file.renameTo(f);
        this.file = f;
        return success;
    }

    public File getFile() {
        return file;
    }
}
