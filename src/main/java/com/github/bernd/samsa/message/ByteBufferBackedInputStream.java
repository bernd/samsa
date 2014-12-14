package com.github.bernd.samsa.message;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {
    private final ByteBuffer buffer;

    public ByteBufferBackedInputStream(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        return buffer.hasRemaining() ? (buffer.get() & 0xFF) : -1;
    }
    @Override
    public int read(byte[] bytes, final int off, final int len) {
        if (buffer.hasRemaining()) {
            // Read only what's left
            int realLen = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, realLen);
            return realLen;
        } else {
            return -1;
        }
    }
}
