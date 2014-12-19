package com.github.bernd.samsa.message;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

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
}
