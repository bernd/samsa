package com.github.bernd.samsa;

import java.nio.ByteBuffer;

public interface OffsetMap {
    int getSlots();

    void put(ByteBuffer key, long offset) throws OffsetMapException;

    long get(ByteBuffer key) throws OffsetMapException;

    void clear();

    int getSize();

    double getUtilization();
}
