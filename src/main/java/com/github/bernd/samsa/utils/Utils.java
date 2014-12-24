package com.github.bernd.samsa.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     * Read an unsigned integer from the current position in the buffer,
     * incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(final ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers
     * position
     *
     * @param buffer the buffer to read from
     * @param index  the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(final ByteBuffer buffer, final int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value  The value to write
     */
    public static void writeUnsignedInt(final ByteBuffer buffer, final long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index  The position in the buffer at which to begin writing
     * @param value  The value to write
     */
    public static void writeUnsignedInt(final ByteBuffer buffer, final int index, final long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Read a big-endian integer from a byte array
     */
    public static int readInt(final byte[] bytes, final int offset) {
        return ((bytes[offset] & 0xFF) << 24) |
                ((bytes[offset + 1] & 0xFF) << 16) |
                ((bytes[offset + 2] & 0xFF) << 8) |
                (bytes[offset + 3] & 0xFF);
    }

    /**
     * Compute the CRC32 of the byte array
     *
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public static long crc32(final byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
     *
     * @param bytes  The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size   the number of bytes to checksum
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
        if (!s.endsWith(oldSuffix)) {
            throw new IllegalArgumentException(String.format("Expected string to end with '%s' but string is '%s'", oldSuffix, s));
        }
        return s.substring(0, s.length() - oldSuffix.length()) + newSuffix;
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0. This is different from
     * java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        if (n == Integer.MIN_VALUE) {
            return 0;
        } else {
            return Math.abs(n);
        }
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(final String file) {
        rm(new File(file));
    }

    /**
     * Recursively delete the list of files/directories and any subfiles (if any exist)
     *
     * @param files sequence of files to be deleted
     */
    public static void rm(final List<String> files) {
        for (String file : files) {
            rm(new File(file));
        }

    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(final File file) {
        if (file == null) {
            return;
        } else if (file.isDirectory()) {
            final File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    rm(f);
                }
            }
            file.delete();
        } else {
            file.delete();
        }
    }

    public static void require(final boolean requirement) {
        if (!requirement) {
            throw new IllegalArgumentException("requirement failed");
        }
    }

    public static void require(final boolean requirement, final String message) {
        if (!requirement) {
            throw new IllegalArgumentException("requirement failed: " + message);
        }
    }

    /**
     * Create a new thread
     *
     * @param name     The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon   Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread(final String name, final Runnable runnable, final boolean daemon) {
        final Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(final Thread t, final Throwable e) {
                LOG.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }

    /**
     * Read the given byte buffer into a byte array
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return readBytes(buffer, 0, buffer.limit());
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     */
    public static byte[] readBytes(final ByteBuffer buffer, final int offset, final int size) {
        byte[] dest = new byte[size];

        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            buffer.mark();
            buffer.get(dest);
            buffer.reset();
        }
        return dest;
    }

    /**
     * Translate the given buffer into a string
     *
     * @param buffer   The buffer to translate
     * @param encoding The encoding to use in translating bytes to characters
     */
    public static String readString(final ByteBuffer buffer, final String encoding) throws UnsupportedEncodingException {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, encoding);
    }

    public static String readString(final ByteBuffer buffer) throws UnsupportedEncodingException {
        return readString(buffer, Charset.defaultCharset().toString());
    }
}
