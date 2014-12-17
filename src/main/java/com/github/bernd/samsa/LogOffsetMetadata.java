package com.github.bernd.samsa;

public class LogOffsetMetadata {
    public static final LogOffsetMetadata UNKNOWN_OFFSET_METADATA = new LogOffsetMetadata(-1, 0, 0);
    private static final long UNKNOWN_SEG_BASE_OFFSET = -1L;
    private static final int UNKNOWN_FILE_POSITION = -1;
    private final long messageOffset;
    private final long segmentBaseOffset;
    private final int relativePositionInSegment;

    public LogOffsetMetadata(final long messageOffset,
                             final long segmentBaseOffset,
                             final int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    // check if this offset is already on an older segment compared with the given offset
    public boolean offsetOnOlderSegment(final LogOffsetMetadata that) {
        if (messageOffsetOnly()) {
            throw new SamsaException(String.format("%s cannot compare its segment info with %s since it only has message offset info", this, that));
        }

        return segmentBaseOffset < that.segmentBaseOffset;
    }

    // check if this offset is on the same segment with the given offset
    public boolean offsetOnSameSegment(final LogOffsetMetadata that) {
        if (messageOffsetOnly()) {
            throw new SamsaException(String.format("%s cannot compare its segment info with %s since it only has message offset info", this, that));
        }

        return segmentBaseOffset == that.segmentBaseOffset;
    }

    // check if this offset is before the given offset
    public boolean precedes(final LogOffsetMetadata that) {
        return messageOffset < that.messageOffset;
    }

    // compute the number of messages between this offset to the given offset
    public long offsetDiff(final LogOffsetMetadata that) {
        return messageOffset - that.messageOffset;
    }

    // compute the number of bytes between this offset to the given offset
    // if they are on the same segment and this offset precedes the given offset
    public int positionDiff(final LogOffsetMetadata that) {
        if (!offsetOnSameSegment(that)) {
            throw new SamsaException(String.format("%s cannot compare its segment position with %s since they are not on the same segment", this, that));
        }
        if (messageOffsetOnly()) {
            throw new SamsaException(String.format("%s cannot compare its segment position with %s since it only has message offset info", this, that));
        }

        return relativePositionInSegment - that.relativePositionInSegment;
    }

    // decide if the offset metadata only contains message offset info
    private boolean messageOffsetOnly() {
        return segmentBaseOffset == UNKNOWN_SEG_BASE_OFFSET &&
                relativePositionInSegment == UNKNOWN_FILE_POSITION;
    }

    @Override
    public String toString() {
      return messageOffset + " [" + segmentBaseOffset + " : " + relativePositionInSegment + "]";
    }

    public long getMessageOffset() {
        return messageOffset;
    }
}
