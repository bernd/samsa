package com.github.bernd.samsa.utils;

import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AbstractIteratorTest {
    private AbstractIterator<Integer> iterator;
    private List<Integer> list;

    @BeforeMethod
    public void setUp() throws Exception {
        list = Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        iterator = new AbstractIterator<Integer>() {
            int i = 0;

            @Override
            protected Integer makeNext() {
                if (i >= list.size()) {
                    return allDone();
                } else {
                    int item = list.get(i);
                    i += 1;
                    return item;
                }
            }
        };
    }

    @Test
    public void testIterator() throws Exception {
        for (int i = 0; i < 10; i++) {
            assertEquals(iterator.hasNext(), true, "We should have an item to read.");
            assertEquals(iterator.hasNext(), true, "Checking again shouldn't change anything.");
            assertTrue(iterator.peek() == i, "Peeking at the item should show the right thing.");
            assertTrue(iterator.peek() == i, "Peeking again shouldn't change anything");
            assertTrue(iterator.next() == i, "Getting the item should give the right thing.");
        }
        assertEquals(iterator.hasNext(), false, "All gone!");

        int catched = 0;

        try {
            iterator.peek();
        } catch (NoSuchElementException e) {
            catched++;
        }
        try {
            iterator.peek();
        } catch (NoSuchElementException e) {
            catched++;
        }

        assertEquals(catched, 2);
    }
}