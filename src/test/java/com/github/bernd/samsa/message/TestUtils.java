package com.github.bernd.samsa.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

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
}
