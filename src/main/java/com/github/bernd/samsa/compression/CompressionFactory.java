package com.github.bernd.samsa.compression;

import java.io.InputStream;
import java.io.OutputStream;

public class CompressionFactory {
    public static OutputStream create(final CompressionCodec compressionCodec, final OutputStream stream) {
        switch (compressionCodec) {
            case NONE:
                return stream;
            case GZIP:
                // return new GZIPOutputStream(stream);
            case SNAPPY:
            case LZ4:
            default:
                throw new UnknownCodecException("Unknown Codec: " + compressionCodec);
        }
    }

    public static InputStream create(final CompressionCodec compressionCodec, final InputStream stream) {
        switch (compressionCodec) {
            case NONE:
                return stream;
            case GZIP:
                // return new GZIPInputStream(stream);
            case SNAPPY:
            case LZ4:
            default:
                throw new UnknownCodecException("Unknown Codec: " + compressionCodec);
        }
    }
}
