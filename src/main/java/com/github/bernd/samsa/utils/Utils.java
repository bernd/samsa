package com.github.bernd.samsa.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class Utils {
    /**
     * Read an unsigned integer from the current position in the buffer,
     * incrementing the position by 4 bytes
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(final ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers
     * position
     * @param buffer the buffer to read from
     * @param index the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(final ByteBuffer buffer, final int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

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

    /**
     * Open a channel for the given file
     */
    public static FileChannel openChannel(final File file, final boolean mutable) throws FileNotFoundException {
        if (mutable) {
            return new RandomAccessFile(file, "rw").getChannel();
        } else {

            return new FileInputStream(file).getChannel();
        }
    }

    /**
     * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
     */
    public static String replaceSuffix(final String s, final String oldSuffix, final String newSuffix) {
        if (! s.endsWith(oldSuffix)) {
            throw new IllegalArgumentException(String.format("Expected string to end with '%s' but string is '%s'", oldSuffix, s));
        }
        return s.substring(0, s.length() - oldSuffix.length()) + newSuffix;
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is different from
     * java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        return n & 0x7fffffff;
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     * @param file The root file at which to begin deleting
     */
    public static void rm(final String file) {
        rm(new File(file));
    }

    /**
     * Recursively delete the list of files/directories and any subfiles (if any exist)
     * @param files sequence of files to be deleted
     */
    public static void rm(final List<String> files) {
        for (String file : files) {
            rm(new File(file));
        }

    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     * @param file The root file at which to begin deleting
     */
    public static void rm(final File file) {
        if (file == null) {
            return;
        } else if (file.isDirectory()) {
            final File[] files = file.listFiles();
            if (files != null) {
                for (File f: files) {
                    rm(f);
                }
            }
            file.delete();
        } else {
            file.delete();
        }
    }
}
