package com.github.bernd.samsa;

import java.nio.ByteBuffer;

public interface OffsetMap {
    public int getSlots();

    public void put(ByteBuffer key, long offset) throws OffsetMapException;

    public long get(ByteBuffer key) throws OffsetMapException;

    public void clear();

    public int getSize();

    public double getUtilization();
}
