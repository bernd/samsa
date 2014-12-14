package com.github.bernd.samsa;

import com.github.bernd.samsa.message.ByteBufferMessageSet;

public class FetchDataInfo {
    private final LogOffsetMetadata offsetMetadata;
    private final ByteBufferMessageSet empty;

    public FetchDataInfo(final LogOffsetMetadata offsetMetadata,
                         final ByteBufferMessageSet empty) {
        this.offsetMetadata = offsetMetadata;
        this.empty = empty;
    }

    public LogOffsetMetadata getOffsetMetadata() {
        return offsetMetadata;
    }

    public ByteBufferMessageSet getEmpty() {
        return empty;
    }
}
