package com.github.bernd.samsa;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.message.ByteBufferMessageSet;
import com.github.bernd.samsa.message.Message;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private static final String IO_TMP_DIR = System.getProperty("java.io.tmpdir");
    private static final Random RANDOM = new Random();

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
            assertFalse(true, "Iterators have uneven length-- first has more: "+length1 + " > " + length);
        }

        // check if the actual iterator was longer
        if (actual.hasNext()) {
            int length2 = length;
            while (actual.hasNext()) {
                actual.next();
                length2 += 1;
            }
            assertFalse(true, "Iterators have uneven length-- second has more: "+length2 + " > " + length);
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
     * Wrap the message in a message set
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
}
