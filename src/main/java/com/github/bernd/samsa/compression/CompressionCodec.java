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

    public static CompressionCodec fromValue(int value) {
        switch (value) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            default:
                throw new InvalidCompressionCodec();
        }
    }

    private static class InvalidCompressionCodec extends RuntimeException {
    }
}
