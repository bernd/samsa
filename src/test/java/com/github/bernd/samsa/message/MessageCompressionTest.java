package com.github.bernd.samsa.message;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageCompressionTest {
    @Test
    public void testSimpleCompressDecompress() throws Exception {
        final List<CompressionCodec> codecs = Lists.newArrayList(CompressionCodec.GZIP);

        if (isSnappyAvailable()) {
            codecs.add(CompressionCodec.SNAPPY);
        }

        // TODO Implement LZ4 compression/decompression.
        /*
        if(isLZ4Available()) {
            codecs.add(CompressionCodec.LZ4);
        }
        */

        for (final CompressionCodec codec : codecs) {
            testSimpleCompressDecompress(codec);
        }
    }

    public void testSimpleCompressDecompress(final CompressionCodec compressionCodec) throws Exception {
        final List<Message> messages = Lists.newArrayList(
                new Message("hi there".getBytes()),
                new Message("I am fine".getBytes()),
                new Message("I am not so well today".getBytes())
        );
        final ByteBufferMessageSet messageSet = new ByteBufferMessageSet(compressionCodec, messages);
        assertEquals(messageSet.shallowIterator().next().getMessage().compressionCodec(), compressionCodec);
        final List<Message> decompressed = Lists.newArrayList();
        for (MessageAndOffset aMessageSet : messageSet) {
            decompressed.add(aMessageSet.getMessage());
        }
        assertEquals(messages, decompressed);
    }

    private boolean isSnappyAvailable() {
        try {
            final org.xerial.snappy.SnappyOutputStream stream = new org.xerial.snappy.SnappyOutputStream(new ByteArrayOutputStream());
            return true;
        } catch (UnsatisfiedLinkError | org.xerial.snappy.SnappyError e) {
            return false;
        }
    }

    private boolean isLZ4Available() {
        try {
            final net.jpountz.lz4.LZ4BlockOutputStream stream = new net.jpountz.lz4.LZ4BlockOutputStream(new ByteArrayOutputStream());
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }
}
