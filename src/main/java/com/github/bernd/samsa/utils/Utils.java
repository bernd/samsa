package com.github.bernd.samsa.utils;

import java.nio.ByteBuffer;

public class Utils {
    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * @param buffer The buffer to write to
     * @param value The value to write
     */
    public static void writeUnsignedInt(final ByteBuffer buffer, final long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     * @param buffer The buffer to write to
     * @param index The position in the buffer at which to begin writing
     * @param value The value to write
     */
    public static void writeUnsignedInt(final ByteBuffer buffer, final int index, final long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Compute the CRC32 of the byte array
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public static long crc32(final byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
     * @param bytes The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size the number of bytes to checksum
     * @return The CRC32
     */
    public static long crc32(final byte[] bytes, final int offset, final int size) {
        final Crc32 crc32 = new Crc32();
        crc32.update(bytes, offset, size);
        return crc32.getValue();
    }
}
