package com.github.bernd.samsa;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.utils.Utils;

import java.nio.ByteBuffer;

/**
 * A message. The format of an N byte message is the following:
 *
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 0 currently
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4. 4 byte key length, containing length K
 * 5. K byte key
 * 6. 4 byte payload length, containing length V
 * 7. V byte payload
 *
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 */
public class Message {
    /**
     * The current offset and size for all the fixed-length fields
     */
    private static final int CRC_OFFSET = 0;
    private static final int CRC_LENGTH = 4;
    private static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    private static final int MAGIC_LENGTH = 1;
    private static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    private static final int ATTRIBUTES_LENGTH = 1;
    private static final int KEY_SIZE_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    private static final int KEY_SIZE_LENGTH = 4;
    private static final int KEY_OFFSET = KEY_SIZE_OFFSET + KEY_SIZE_LENGTH;
    private static final int VALUE_SIZE_LENGTH = 4;

    /** The amount of overhead bytes in a message */
    private static final int MESSAGE_OVERHEAD = KEY_OFFSET + VALUE_SIZE_LENGTH;

    /**
     * The minimum valid size for the message header
     */
    private static final int MIN_HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The current "magic" value
     */
    private static final byte CURRENT_MAGIC_VALUE = 0;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    private static final int COMPRESSION_CODE_MASK = 0x07;

    /**
     * Compression code for uncompressed messages
     */
    private static final int NO_COMPRESSION = 0;


    private final ByteBuffer buffer;

    /**
     * A constructor to create a Message
     * @param bytes The payload of the message
     * @param key The key of the message (null, if none)
     * @param compressionCodec The compression codec used on the contents of the message (if any)
     * @param payloadOffset The offset into the payload array used to extract payload
     * @param payloadSize The size of the payload to use
     */
    public Message(final byte[] bytes,
                   final byte[] key,
                   final CompressionCodec compressionCodec,
                   final int payloadOffset,
                   final int payloadSize) {
        int valueSizeLength;

        if (bytes == null) {
            valueSizeLength = 0;
        } else if (payloadSize >= 0) {
            valueSizeLength = payloadSize;
        } else {
            valueSizeLength = bytes.length - payloadOffset;
        }

        this.buffer = ByteBuffer.allocate(CRC_LENGTH +
                MAGIC_LENGTH +
                ATTRIBUTES_LENGTH +
                KEY_SIZE_LENGTH +
                (key == null ? 0 : key.length) +
                VALUE_SIZE_LENGTH +
                valueSizeLength);

        // skip crc, we will fill that in at the end
        buffer.position(MAGIC_OFFSET);
        buffer.put(CURRENT_MAGIC_VALUE);

        byte attributes = 0;

        if (compressionCodec.getValue() > 0) {
            attributes = (byte) (attributes | (COMPRESSION_CODE_MASK & compressionCodec.getValue()));
        }
        buffer.put(attributes);

        if (key == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(key.length);
            buffer.put(key, 0, key.length);
        }

        int size;

        if (bytes == null) {
            size = -1;
        } else if (payloadSize >= 0) {
            size = payloadSize;
        } else {
            size = bytes.length - payloadOffset;
        }

        buffer.putInt(size);
        if (bytes != null) {
            buffer.put(bytes, payloadOffset, size);
        }

        buffer.rewind();

        // now compute the checksum and fill it in
        Utils.writeUnsignedInt(buffer, CRC_OFFSET, computeChecksum());
    }

    public Message(final byte[] bytes, final byte[] key, final CompressionCodec compressionCodec) {
        this(bytes, key, compressionCodec, 0, -1);
    }

    public Message(final byte[] bytes, final CompressionCodec compressionCodec) {
        this(bytes, null, compressionCodec);
    }

    public Message(final byte[] bytes, final byte[] key) {
        this(bytes, key, CompressionCodec.NONE);
    }

    public Message(final byte[] bytes) {
        this(bytes, null, CompressionCodec.NONE);
    }

    private long computeChecksum() {
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }
}
