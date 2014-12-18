package com.github.bernd.samsa;

import com.github.bernd.samsa.utils.Os;
import com.github.bernd.samsa.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * <p/>
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * <p/>
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * <p/>
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * <p/>
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * <p/>
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * <p/>
 * The frequency of entries is up to the user of this class.
 * <p/>
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public class OffsetIndex {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetIndex.class);

    private final ReentrantLock lock = new ReentrantLock();
    private long lastOffset;
    private volatile File file; // Was marked as @volatile in Kafka
    private final long baseOffset;
    private final int maxIndexSize;

    private MappedByteBuffer mmap;
    private final AtomicInteger size;

    /**
     * The maximum number of eight-byte entries this index can hold
     */
    private volatile int maxEntries;

    public OffsetIndex(final File file, final long baseOffset, final int maxIndexSize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;

        /* initialize the memory mapping for this index */
        this.mmap = mmap();

        /* the number of eight-byte entries currently in the index */
        this.size = new AtomicInteger(mmap.position() / 8);

        this.maxEntries = mmap.limit() / 8;

        /* the last offset in the index */
        this.lastOffset = readLastEntry().getOffset();

        LOG.debug(String.format("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d",
                file.getAbsolutePath(), maxEntries, maxIndexSize, entries(), lastOffset, mmap.position()));
    }

    private MappedByteBuffer mmap() throws IOException {
        boolean newlyCreated = file.createNewFile();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        try {
            /* pre-allocate the file if necessary */
            if (newlyCreated) {
                if (maxIndexSize < 8) {
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                }
                raf.setLength(roundToExactMultiple(maxIndexSize, 8));
            }

            /* memory-map the file */
            long len = raf.length();
            MappedByteBuffer idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);

            /* set the position in the index for the next entry */
            if (newlyCreated) {
                idx.position(0);
            } else {
                // if this is a pre-existing index, assume it is all valid and set position to last entry
                idx.position(roundToExactMultiple(idx.limit(), 8));
            }
            return idx;
        } finally {
            // Used to be thi in Kafka: Utils.swallow(raf.close());
            try {
                raf.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    /**
     * The last entry in the index
     */
    private OffsetPosition readLastEntry() {
        lock.lock();
        try {
            int s = size.get();
            if (s == 0) {
                return new OffsetPosition(baseOffset, 0);
            } else {
                return new OffsetPosition(baseOffset + relativeOffset(mmap, s - 1), physical(mmap, s - 1));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and it's corresponding physical file position.
     *
     * @param targetOffset The offset to look up.
     * @return The offset found and the corresponding file position for this offset.
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(final long targetOffset) {
        if (Os.isWindows()) {
            lock.lock();
        }
        try {
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, targetOffset);
            if (slot == -1) {
                return new OffsetPosition(baseOffset, 0);
            } else {
                return new OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot));
            }
        } finally {
            if (Os.isWindows()) {
                lock.unlock();
            }
        }
    }

    /**
     * Find the slot in which the largest offset less than or equal to the given
     * target offset is stored.
     *
     * @param idx          The index buffer
     * @param targetOffset The offset to look for
     * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
     */
    private int indexSlotFor(final ByteBuffer idx, final long targetOffset) {
        // we only store the difference from the base offset so calculate that
        long relOffset = targetOffset - baseOffset;

        // check if the index is empty
        if (entries() == 0) {
            return -1;
        }

        // check if the target offset is smaller than the least offset
        if (relativeOffset(idx, 0) > relOffset) {
            return -1;
        }

        // binary search for the entry
        int lo = 0;
        int hi = entries() - 1;
        while (lo < hi) {
            int mid = (int) Math.ceil(hi / 2.0 + lo / 2.0);
            int found = relativeOffset(idx, mid);
            if (found == relOffset) {
                return mid;
            } else if (found < relOffset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /* return the nth offset relative to the base offset */
    private int relativeOffset(final ByteBuffer buffer, final int n) {
        return buffer.getInt(n * 8);
    }

    /* return the nth physical position */
    private int physical(final ByteBuffer buffer, final int n) {
        return buffer.getInt(n * 8 + 4);
    }

    /**
     * Get the nth offset mapping from the index
     *
     * @param n The entry number in the index
     * @return The offset/position pair at that entry
     */
    public OffsetPosition entry(final int n) {
        if (Os.isWindows()) {
            lock.lock();
        }
        try {
            if (n >= entries()) {
                throw new IllegalArgumentException(String.format("Attempt to fetch the %dth entry from an index of size %d.", n, entries()));
            }
            ByteBuffer idx = mmap.duplicate();
            return new OffsetPosition(relativeOffset(idx, n), physical(idx, n));
        } finally {
            if (Os.isWindows()) {
                lock.unlock();
            }
        }
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     */
    public void append(final long offset, final int position) {
        lock.lock();
        try {
            Utils.require(!isFull(), "Attempt to append to a full index (size = " + size + ").");
            if (size.get() == 0 || offset > lastOffset) {
                LOG.debug(String.format("Adding index entry %d => %d to %s.", offset, position, file.getName()));
                mmap.putInt((int) (offset - baseOffset));
                mmap.putInt(position);
                size.incrementAndGet();
                lastOffset = offset;
                Utils.require(entries() * 8 == mmap.position(), entries() + " entries but file position in index is " + mmap.position() + ".");
            } else {
                throw new InvalidOffsetException(String.format("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s.",
                        offset, entries(), lastOffset, file.getAbsolutePath()));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * True if there are no more slots available in this index
     */
    public boolean isFull() {
        return entries() >= maxEntries;
    }

    /**
     * Truncate the entire index, deleting all entries
     */
    public void truncate() {
        truncateToEntries(0);
    }

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public void truncateTo(final long offset) {
        lock.lock();
        try {
            final ByteBuffer idx = mmap.duplicate();
            final int slot = indexSlotFor(idx, offset);

            /* There are 3 cases for choosing the new size
             * 1) if there is no entry in the index <= the offset, delete everything
             * 2) if there is an entry for this exact offset, delete it and everything larger than it
             * 3) if there is no entry for this offset, delete everything larger than the next smallest
             */
            int newEntries;
            if (slot < 0) {
                newEntries = 0;
            } else if (relativeOffset(idx, slot) == offset - baseOffset) {
                newEntries = slot;
            } else {
                newEntries = slot + 1;
            }
            truncateToEntries(newEntries);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(final int entries) {
        lock.lock();
        try {
            size.set(entries);
            mmap.position(size.get() * 8);
            lastOffset = readLastEntry().getOffset();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() throws IOException {
        lock.lock();
        try {
            resize(entries() * 8);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     */
    public void resize(final int newSize) throws IOException {
        lock.lock();
        try {
            final RandomAccessFile raf = new RandomAccessFile(file, "rws");
            final int roundedNewSize = roundToExactMultiple(newSize, 8);
            final int position = mmap.position();

            /* Windows won't let us modify the file length while the file is mmapped :-( */
            if (Os.isWindows()) {
                forceUnmap(mmap);
            }
            try {
                raf.setLength(roundedNewSize);
                mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                this.maxEntries = mmap.limit() / 8;
                mmap.position(position);
            } finally {
                // This was: Utils.swallow(raf.close())
                try {
                    raf.close();
                } catch (Exception e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Forcefully free the buffer's mmap. We do this only on windows.
     */
    private void forceUnmap(final MappedByteBuffer m) {
        try {
            if (m instanceof sun.nio.ch.DirectBuffer) {
                ((sun.nio.ch.DirectBuffer) m).cleaner().clean();
            }
        } catch (Exception e) {
            LOG.warn("Error when freeing index buffer", e);
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        lock.lock();
        try {
            mmap.force();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Delete this index file
     */
    public boolean delete() {
        LOG.info("Deleting index {}", file.getAbsolutePath());
        return file.delete();
    }

    /**
     * The number of entries in this index
     */
    public int entries() {
        return size.get();
    }

    /**
     * The number of bytes actually used by this index
     */
    public int sizeInBytes() {
        return 8 * entries();
    }

    /**
     * Close the index
     */
    public void close() throws IOException {
        trimToValidSize();
    }

    /**
     * Rename the file that backs this offset index
     *
     * @return true iff the rename was successful
     */
    public boolean renameTo(final File f) {
        final boolean success = file.renameTo(f);
        file = f;
        return success;
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems
     *
     * @throws IllegalArgumentException if any problems are found
     */
    public void sanityCheck() {
        Utils.require(entries() == 0 || lastOffset > baseOffset,
                String.format("Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d",
                        file.getAbsolutePath(), lastOffset, baseOffset));
        long len = file.length();
        Utils.require(len % 8 == 0,
                "Index file " + file.getName() + " is corrupt, found " + len +
                        " bytes which is not positive or not a multiple of 8.");
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundToExactMultiple(67, 8) == 64
     */
    private int roundToExactMultiple(final int number, final int factor) {
        return factor * (number / factor);
    }

    public int getMaxIndexSize() {
        return maxIndexSize;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public File getFile() {
        return file;
    }

    public int getMaxEntries() {
        return maxEntries;
    }
}
