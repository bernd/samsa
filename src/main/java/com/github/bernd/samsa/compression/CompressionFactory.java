package com.github.bernd.samsa.compression;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionFactory {
    public static OutputStream create(final CompressionCodec compressionCodec, final OutputStream stream) throws IOException {
        switch (compressionCodec) {
            case NONE:
                return stream;
            case GZIP:
                return new GZIPOutputStream(stream);
            case SNAPPY:
                return new SnappyOutputStream(stream);
            case LZ4:
                return new KafkaLZ4BlockOutputStream(stream);
            default:
                throw new UnknownCodecException("Unknown Codec: " + compressionCodec);
        }
    }

    public static InputStream create(final CompressionCodec compressionCodec, final InputStream stream) throws IOException {
        switch (compressionCodec) {
            case NONE:
                return stream;
            case GZIP:
                return new GZIPInputStream(stream);
            case SNAPPY:
                return new SnappyInputStream(stream);
            case LZ4:
                return  new KafkaLZ4BlockInputStream(stream);
            default:
                throw new UnknownCodecException("Unknown Codec: " + compressionCodec);
        }
    }
}
