package com.github.bernd.samsa;

public class OffsetPosition {
    private final long offset;
    private final int position;

    public OffsetPosition(final long offset, final int position) {
        this.offset = offset;
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OffsetPosition)) return false;

        OffsetPosition that = (OffsetPosition) o;

        if (offset != that.offset) return false;
        if (position != that.position) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + position;
        return result;
    }

    @Override
    public String toString() {
        return "OffsetPosition{" +
                "offset=" + offset +
                ", position=" + position +
                '}';
    }

    public long getOffset() {
        return offset;
    }

    public int getPosition() {
        return position;
    }
}
