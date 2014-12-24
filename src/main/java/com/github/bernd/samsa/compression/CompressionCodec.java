package com.github.bernd.samsa.compression;

public enum CompressionCodec {
    NONE(0, "none"), GZIP(1, "gzip"), SNAPPY(2, "snappy"), LZ4(3, "lz4");

    public final int codec;
    public final String name;

    CompressionCodec(final int codec, final String name) {
        this.codec = codec;
        this.name = name;
    }

    public int getCodec() {
        return codec;
    }

    public static CompressionCodec fromCodec(int codec) {
        switch (codec) {
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

    private static CompressionCodec fromName(String codec) {
        switch (codec.toLowerCase()) {
            case "none":
                return NONE;
            case "gzip":
                return GZIP;
            case "snappy":
                return SNAPPY;
            case "lz4":
                return LZ4;
            default:
                throw new InvalidCompressionCodec();
        }
    }


    public static CompressionCodec getCompressionCodec(final int codec) {
        return fromCodec(codec);
    }

    public static CompressionCodec getCompressionCodec(final String codec) {
        return fromName(codec);
    }

    private static class InvalidCompressionCodec extends RuntimeException {
    }
}
