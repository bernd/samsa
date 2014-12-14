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
    public static final int MIN_HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;

    /**
     * The current "magic" value
     */
    private static final byte CURRENT_MAGIC_VALUE = 0;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    private static final int COMPRESSION_CODEC_MASK = 0x07;

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
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & compressionCodec.getValue()));
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

    /**
     * Compute the checksum of the message from the message contents
     */
    private long computeChecksum() {
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

    /**
     * Retrieve the previously computed CRC for this message
     */
    public long checksum() {
        return Utils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    /**
     * Returns true if the crc stored with the message matches the crc computed off the message contents
     */
    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidMessageException if isValid is false for this message
     */
    public void ensureValid() {
        if(! isValid()) {
            throw new InvalidMessageException("Message is corrupt (stored crc = " + checksum() + ", computed crc = " + computeChecksum() + ")");
        }
    }

    /**
     * The complete serialized size of this message in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        return buffer.getInt(KEY_SIZE_OFFSET);
    }

    /**
     * Does the message have a key?
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the payload size is stored
     */
    private int payloadSizeOffset() {
        return KEY_OFFSET + Math.max(0, keySize());
    }

    /**
     * The length of the message value in bytes
     */
    public int payloadSize() {
        return buffer.getInt(payloadSizeOffset());
    }

    /**
     * Is the payload of this message null
     */
    public boolean isNull() {
        return payloadSize() < 0;
    }

    /**
     * The magic version of this message
     */
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    /**
     * The attributes stored with this message
     */
    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    /**
     * The compression codec used with this message
     */
    public CompressionCodec compressionCodec() {
        return CompressionCodec.fromValue(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    /**
     * A ByteBuffer containing the content of the message
     */
    public ByteBuffer payload() {
        return sliceDelimited(payloadSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        return sliceDelimited(KEY_SIZE_OFFSET);
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(final int start) {
        final int size = buffer.getInt(start);

        if (size < 0) {
            return null;
        } else {
            ByteBuffer buf = buffer.duplicate();
            buf.position(start + 4);
            buf = buf.slice();
            buf.limit(size);
            buf.rewind();
            return buf;
        }
    }

    @Override
    public String toString() {
        return String.format("Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)",
                magic(), attributes(), checksum(), key(), payload());
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Message && buffer.equals(((Message) other).buffer());
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }
}
