package com.github.bernd.samsa;

public class OffsetPosition {
    private final long offset;
    private final int position;

    public OffsetPosition(final long offset, final int position) {
        this.offset = offset;
        this.position = position;
    }

    public long getOffset() {
        return offset;
    }

    public int getPosition() {
        return position;
    }
}
