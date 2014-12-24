package com.github.bernd.samsa;

import com.github.bernd.samsa.utils.Utils;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * An hash table used for deduplicating the log. This hash table uses a cryptographicly secure hash of the key as a proxy for the key
 * for comparisons and to save space on object overhead. Collisions are resolved by probing. This hash table does not support deletes.
 * <p/>
 * No thread-safe!
 */
public class SkimpyOffsetMap implements OffsetMap {
    private final ByteBuffer bytes;
    private final MessageDigest digest;
    private final int hashSize;
    private final byte[] hash1;
    private final byte[] hash2;
    private int entries;
    private long lookups;
    private long probes;
    private final int bytesPerEntry;
    private final int slots;

    public SkimpyOffsetMap(final int memory) throws NoSuchAlgorithmException {
        this(memory, "MD5");
    }

    /**
     * @param memory        The amount of memory this map can use
     * @param hashAlgorithm The hash algorithm instance to use: MD2, MD5, SHA-1, SHA-256, SHA-384, SHA-512
     */
    public SkimpyOffsetMap(final int memory, final String hashAlgorithm) throws NoSuchAlgorithmException {
        this.bytes = ByteBuffer.allocate(memory);

        /* the hash algorithm instance to use, defualt is MD5 */
        this.digest = MessageDigest.getInstance(hashAlgorithm);

        /* the number of bytes for this hash algorithm */
        this.hashSize = digest.getDigestLength();

        /* create some hash buffers to avoid reallocating each time */
        this.hash1 = new byte[hashSize];
        this.hash2 = new byte[hashSize];

        /* number of entries put into the map */
        this.entries = 0;

        /* number of lookups on the map */
        this.lookups = 0L;

        /* the number of probes for all lookups */
        this.probes = 0L;

        /**
         * The number of bytes of space each entry uses (the number of bytes in the hash plus an 8 byte offset)
         */
        this.bytesPerEntry = hashSize + 8;

        /**
         * The maximum number of entries this map can contain
         */
        this.slots = (memory / bytesPerEntry);
    }

    @Override
    public int getSlots() {
        return 0;
    }

    /**
     * Associate this offset to the given key.
     *
     * @param key    The key
     * @param offset The offset
     */
    @Override
    public void put(ByteBuffer key, long offset) throws OffsetMapException {
        Utils.require(entries < slots, "Attempt to add a new entry to a full offset map.");
        lookups += 1;
        hashInto(key, hash1);
        // probe until we find the first empty slot
        int attempt = 0;
        int pos = positionOf(hash1, attempt);
        while (!isEmpty(pos)) {
            bytes.position(pos);
            bytes.get(hash2);
            if (Arrays.equals(hash1, hash2)) {
                // we found an existing entry, overwrite it and return (size does not change)
                bytes.putLong(offset);
                return;
            }
            attempt += 1;
            pos = positionOf(hash1, attempt);
        }
        // found an empty slot, update it--size grows by 1
        bytes.position(pos);
        bytes.put(hash1);
        bytes.putLong(offset);
        entries += 1;
    }

    /**
     * Check that there is no entry at the given position
     */
    private boolean isEmpty(final int position) {
        return bytes.getLong(position) == 0 && bytes.getLong(position + 8) == 0 && bytes.getLong(position + 16) == 0;
    }

    /**
     * Get the offset associated with this key.
     *
     * @param key The key
     * @return The offset associated with this key or -1 if the key is not found
     */
    @Override
    public long get(ByteBuffer key) throws OffsetMapException {
        lookups += 1;
        hashInto(key, hash1);
        // search for the hash of this key by repeated probing until we find the hash we are looking for or we find an empty slot
        int attempt = 0;
        int pos = 0;
        do {
            pos = positionOf(hash1, attempt);
            bytes.position(pos);
            if (isEmpty(pos))
                return -1L;
            bytes.get(hash2);
            attempt += 1;
        } while (!Arrays.equals(hash1, hash2));
        return bytes.getLong();
    }

    @Override
    public void clear() {
        entries = 0;
        lookups = 0L;
        probes = 0L;
        Arrays.fill(bytes.array(), bytes.arrayOffset(), bytes.arrayOffset() + bytes.limit(), (byte) 0);
    }

    @Override
    public int getSize() {
        return entries;
    }

    /**
     * The rate of collisions in the lookups
     */
    public double getCollisionRate() {
        return (probes - lookups) / (double) lookups;
    }

    @Override
    public double getUtilization() {
        return (probes - lookups) / (double) lookups;
    }

    /**
     * Calculate the ith probe position. We first try reading successive integers from the hash itself
     * then if all of those fail we degrade to linear probing.
     *
     * @param hash    The hash of the key to find the position for
     * @param attempt The ith probe
     * @return The byte offset in the buffer at which the ith probing for the given hash would reside
     */
    private int positionOf(final byte[] hash, final int attempt) {
        final int probe = Utils.readInt(hash, Math.min(attempt, hashSize - 4)) + Math.max(0, attempt - hashSize + 4);
        final int slot = Utils.abs(probe) % slots;
        this.probes += 1;
        return slot * bytesPerEntry;
    }

    /**
     * The offset at which we have stored the given key
     *
     * @param key    The key to hash
     * @param buffer The buffer to store the hash into
     */
    private void hashInto(final ByteBuffer key, final byte[] buffer) throws OffsetMapException {
        key.mark();
        digest.update(key);
        key.reset();
        try {
            digest.digest(buffer, 0, hashSize);
        } catch (DigestException e) {
            throw new OffsetMapException(e.getMessage(), e);
        }
    }
}
