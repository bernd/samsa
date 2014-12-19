package com.github.bernd.samsa.utils;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

public class UtilsTest {
    @Test
    public void testAbs() throws Exception {
        assertEquals(Utils.abs(Integer.MIN_VALUE), 0);
        assertEquals(Utils.abs(-1), 1);
        assertEquals(Utils.abs(0), 0);
        assertEquals(Utils.abs(1), 1);
        assertEquals(Utils.abs(Integer.MAX_VALUE), Integer.MAX_VALUE);
    }

    @Test
    public void testReadInt() throws Exception {
        final ArrayList<? extends Number> values = Lists.newArrayList(0, 1, -1, Byte.MAX_VALUE, Short.MAX_VALUE,
                2 * Short.MAX_VALUE, Integer.MAX_VALUE / 2, Integer.MIN_VALUE / 2, Integer.MAX_VALUE,
                Integer.MIN_VALUE, Integer.MAX_VALUE);

        final ByteBuffer buffer = ByteBuffer.allocate(4 * values.size());

        for (int i = 0; i < values.size(); i++) {
            buffer.putInt(i * 4, values.get(i).intValue());
            assertEquals(Utils.readInt(buffer.array(), i * 4), values.get(i).intValue(),
                    "Written value should match read value.");
        }
    }
}
