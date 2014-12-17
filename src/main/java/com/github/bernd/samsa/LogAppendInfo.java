package com.github.bernd.samsa;

import com.github.bernd.samsa.compression.CompressionCodec;

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 */
public class LogAppendInfo {
    public long firstOffset;
    public long lastOffset;
    public CompressionCodec codec;
    public int shallowCount;
    public int validBytes;
    public boolean offsetsMonotonic;

    /**
     * @param firstOffset      The first offset in the message set
     * @param lastOffset       The last offset in the message set
     * @param shallowCount     The number of shallow messages
     * @param validBytes       The number of valid bytes
     * @param codec            The codec used in the message set
     * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
     */
    public LogAppendInfo(final long firstOffset,
                         final long lastOffset,
                         final CompressionCodec codec,
                         final int shallowCount,
                         final int validBytes,
                         final boolean offsetsMonotonic) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.codec = codec;
        this.shallowCount = shallowCount;
        this.validBytes = validBytes;
        this.offsetsMonotonic = offsetsMonotonic;
    }
}
