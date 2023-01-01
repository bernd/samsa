package com.github.bernd.samsa;

import com.github.bernd.samsa.cleaner.CleanerConfigBuilder;
import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.utils.MockTime;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private static final String IO_TMP_DIR = System.getProperty("java.io.tmpdir");
    public static final Random RANDOM = new Random();
    public static final Random SEEDED_RANDOM = new Random(192348092834L);

    /**
     * Check that the buffer content from buffer.position() to buffer.limit() is equal
     */
    public static void checkEquals(final ByteBuffer b1, final ByteBuffer b2) {
        assertEquals(b1.limit() - b1.position(), b2.limit() - b2.position(),
                "Buffers should have equal length");
        for (int i = 0; i < (b1.limit() - b1.position()); i++) {
            assertEquals(b1.get(b1.position() + i), b2.get(b1.position() + i),
                    "byte " + i + " byte not equal.");
        }
    }

    /**
     * Throw an exception if the two iterators are of differing lengths or contain
     * different messages on their Nth element
     */
    public static <T> void checkEquals(final Iterator<T> actual, final Iterator<T> expected) {
        int length = 0;
        while (expected.hasNext() && actual.hasNext()) {
            length += 1;
            assertEquals(actual.next(), expected.next());
        }

        // check if the expected iterator is longer
        if (expected.hasNext()) {
            int length1 = length;
            while (expected.hasNext()) {
                expected.next();
                length1 += 1;
            }
            assertFalse(true, "Iterators have uneven length-- first has more: " + length1 + " > " + length);
        }

        // check if the actual iterator was longer
        if (actual.hasNext()) {
            int length2 = length;
            while (actual.hasNext()) {
                actual.next();
                length2 += 1;
            }
            assertFalse(true, "Iterators have uneven length-- second has more: " + length2 + " > " + length);
        }
    }

    /**
     * Execute the given block. If it throws an assert error, retry. Repeat
     * until no error is thrown or the time limit ellapses
     */
    public static void retry(final long maxWaitMs, final Runnable block) {
        long wait = 1L;
        final long startTime = System.currentTimeMillis();
        while (true) {
            try {
                block.run();
                return;
            } catch (AssertionError e) {
                final long ellapsed = System.currentTimeMillis() - startTime;
                if (ellapsed > maxWaitMs) {
                    throw e;
                } else {
                    LOG.info("Attempt failed, sleeping for " + wait + ", and then retrying.");
                    Uninterruptibles.sleepUninterruptibly(wait, TimeUnit.MILLISECONDS);
                    wait += Math.min(wait, 1000);
                }
            }
        }
    }

    /**
     * Create a temporary directory
     */
    public static File tempDir() {
        final File f = new File(IO_TMP_DIR, "samsa-" + RANDOM.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Utils.rm(f);
            }
        });

        return f;
    }

    /**
     * Create a temporary file
     */
    public static File tempFile() throws IOException {
        final File f = File.createTempFile("samsa", ".tmp");
        f.deleteOnExit();
        return f;
    }

    /**
     * Wrap the message in a message set
     *
     * @param payload The bytes of the message
     */
    public static ByteBufferMessageSet singleMessageSet(final byte[] payload,
                                                        final CompressionCodec codec,
                                                        final byte[] key) throws IOException {
        return new ByteBufferMessageSet(codec, Lists.newArrayList(new Message(payload, key)));
    }

    public static ByteBufferMessageSet singleMessageSet(final byte[] payload,
                                                        final CompressionCodec codec) throws IOException {
        return singleMessageSet(payload, codec, null);
    }

    public static ByteBufferMessageSet singleMessageSet(final byte[] payload) throws IOException {
        return singleMessageSet(payload, CompressionCodec.NONE);
    }

    /**
     * Generate an array of random bytes
     *
     * @param numBytes The size of the array
     */
    public static byte[] randomBytes(final int numBytes) {
        byte[] bytes = new byte[numBytes];
        SEEDED_RANDOM.nextBytes(bytes);
        return bytes;
    }

    public static void writeNonsenseToFile(final File fileName, final long position, final int size) throws IOException {
        final RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        file.seek(position);
        for (int i = 0; i < size; i++) {
            file.writeByte(RANDOM.nextInt(255));
        }
        file.close();
    }

    public static void appendNonsenseToFile(final File fileName, final int size) throws IOException {
        final OutputStream file = new FileOutputStream(fileName, true);
        for (int i = 0; i < size; i++) {
            file.write(RANDOM.nextInt(255));
        }
        file.close();
    }

    /**
     * Create new LogManager instance with default configuration for testing
     */
    public static LogManager createLogManager(final List<File> logDirs,
                                              final LogConfig defaultConfig,
                                              final MockTime time) throws Throwable {
        return new LogManager(
                logDirs,
                new HashMap<String, LogConfig>(),
                defaultConfig,
                new CleanerConfigBuilder().enableCleaner(false).build(),
                4,
                1000L,
                10000L,
                1000L,
                time.scheduler,
                new BrokerState(), time);
    }
}
