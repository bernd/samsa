package com.github.bernd.samsa.compression;

public enum CompressionCodec {
    NONE(0), GZIP(1), SNAPPY(2), LZ4(3);

    private final int value;

    CompressionCodec(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
