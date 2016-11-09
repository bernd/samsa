package com.github.bernd.samsa.message;

import com.github.bernd.samsa.commons.TimestampType;
import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;


/**
 * A message. The format of an N byte message is the following:
 * <p>
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 0 or 1
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version
 * bit 0 ~ 2 : Compression codec.
 * 0 : no compression
 * 1 : gzip
 * 2 : snappy
 * 3 : lz4
 * bit 3 : Timestamp type
 * 0 : create time
 * 1 : log append time
 * bit 4 ~ 7 : reserved
 * 4. (Optional) 8 byte timestamp only if "magic" identifier is greater than 0
 * 5. 4 byte key length, containing length K
 * 6. K byte key
 * 7. 4 byte payload length, containing length V
 * 8. V byte payload
 * <p>
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 */
public class Message {
    /**
     * The current offset and size for all the fixed-length fields
     */
    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTES_LENGTH = 1;
    // Only message format version 1 has the timestamp field.
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;
    public static final int VALUE_SIZE_LENGTH = 4;


    private static final ImmutableMap<Byte, Integer> messageHeaderSizeMap = ImmutableMap.<Byte, Integer>builder().put(Byte.valueOf("0"), CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH).put(Byte.valueOf("1"), CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTES_LENGTH + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH).build();

    /**
     * The amount of overhead bytes in a message
     * This value is only used to check if the message size is valid or not. So the minimum possible message bytes is
     * used here, which comes from a message in message format V0 with empty key and value.
     */
    public static final int MIN_MESSAGE_OVERHEAD = KEY_OFFSET_V0 + VALUE_SIZE_LENGTH;


    /**
     * The "magic" value
     * When magic value is 0, the message uses absolute offset and does not have a timestamp field.
     * When magic value is 1, the message uses relative offset and has a timestamp field.
     */
    public static final byte MAGIC_VALUE_V0 = 0;
    public static final byte MAGIC_VALUE_V1 = 1;
    public static final byte CURRENT_MAGIC_VALUE = 1;

    /**
     * Specifies the mask for the compression code. 3 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    public static final int COMPRESSION_CODEC_MASK = 0x07;

    /**
     * Specifies the mask for timestamp type. 1 bit at the 4th least significant bit.
     * 0 for CreateTime, 1 for LogAppendTime
     */
    public static final byte TIMESTAMP_TYPE_MASK = 0x08;
    public static final int TIMESTAMP_TYPE_ATTRIBUTEBIT_OFFSET = 3;

    /**
     * Compression code for uncompressed messages
     */
    public static final int NO_COMPRESSION = 0;

    /**
     * To indicate timestamp is not defined so "magic" value 0 will be used.
     */
    public static final Long NO_TIMESTAMP = -1l;

    private final ByteBuffer buffer;

    private Optional<Long> wrapperMessageTimestamp;

    private Optional<TimestampType> wrapperMessageTimestampType;

    private static int calculateValueSizeLength(final byte[] bytes,
                                                final int payloadSize,
                                                final int payloadOffset) {
        int valueSizeLength;

        if (bytes == null) {
            valueSizeLength = 0;
        } else if (payloadSize >= 0) {
            valueSizeLength = payloadSize;
        } else {
            valueSizeLength = bytes.length - payloadOffset;
        }

        return valueSizeLength;
    }

    private static void validateTimestampAndMagicValue(Long timestamp, Byte magic) {
        if (magic != MAGIC_VALUE_V0 && magic != MAGIC_VALUE_V1)
            throw new IllegalArgumentException(String.format("Invalid magic value %o", magic));
        if (timestamp < 0 && timestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException(String.format("Invalid message timestamp %d", timestamp));
        if (magic == MAGIC_VALUE_V0 && timestamp != NO_TIMESTAMP)
            throw new IllegalArgumentException(String.format("Invalid timestamp %d. Timestamp must be %d when magic = %o", timestamp, NO_TIMESTAMP, MAGIC_VALUE_V0));
    }


    /**
     * Give the header size difference between different message versions.
     */
    public static Integer headerSizeDiff(Byte fromMagicValue, Byte toMagicValue) {
        return messageHeaderSizeMap.get(toMagicValue) - messageHeaderSizeMap.get(fromMagicValue);
    }

    /**
     * A constructor to create a Message
     *
     * @param bytes         The payload of the message
     * @param key           The key of the message (null, if none)
     * @param timeStamp     The timestamp of the message.
     * @param timestampType The timestamp type of the message.
     * @param codec         The compression codec used on the contents of the message (if any)
     * @param payloadOffset The offset into the payload array used to extract payload
     * @param payloadSize   The size of the payload to use
     * @param magicValue    the magic value to use
     */
    public Message(final byte[] bytes,
                   final byte[] key,
                   final Long timeStamp,
                   final TimestampType timestampType,
                   final CompressionCodec codec,
                   final int payloadOffset,
                   final int payloadSize,
                   final byte magicValue) {
        this(ByteBuffer.allocate(CRC_LENGTH +
                MAGIC_LENGTH +
                ATTRIBUTES_LENGTH +
                magicValue == Message.MAGIC_VALUE_V0 ? 0 : Message.ATTRIBUTES_LENGTH +
                KEY_SIZE_LENGTH +
                (key == null ? 0 : key.length) +
                VALUE_SIZE_LENGTH +
                calculateValueSizeLength(bytes, payloadSize, payloadOffset)));
        validateTimestampAndMagicValue(timeStamp, magicValue);
        // skip crc, we will fill that in at the end
        buffer.position(MAGIC_OFFSET);
        buffer.put(magicValue);
        byte attributes = 0;
        if (codec.getCodec() > 0) {
            attributes = timestampType.updateAttributes(COMPRESSION_CODEC_MASK & codec.codec);
        }
        buffer.put(attributes);
        // Only put timestamp when "magic" value is greater than 0
        if (magic() > MAGIC_VALUE_V0) {
            buffer.putLong(timeStamp);
        }
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

    public Message(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Message(final ByteBuffer buffer, final Optional<Long> wrapperMessageTimestamp, final Optional<TimestampType> wrapperMessageTimestampType) {
        this.buffer = buffer;
        this.wrapperMessageTimestamp = wrapperMessageTimestamp;
        this.wrapperMessageTimestampType = wrapperMessageTimestampType;
    }

    public Message(final byte[] bytes, final byte[] key, final Long timestamp, final CompressionCodec compressionCodec, final Byte magicValue) {
        this(bytes, key, timestamp, TimestampType.CREATE_TIME, compressionCodec, 0, -1, magicValue);
    }

    public Message(final byte[] bytes, final Long timeStamp, final CompressionCodec compressionCodec, final Byte magicValue) {
        this(bytes, null, timeStamp, compressionCodec, magicValue);
    }

    public Message(final byte[] bytes, final byte[] key, final Long timeStamp, final Byte magicValue) {
        this(bytes, key, timeStamp, CompressionCodec.NONE, magicValue);
    }

    public Message(final byte[] bytes) {
        this(bytes, null, Message.NO_TIMESTAMP, CompressionCodec.NONE, Message.CURRENT_MAGIC_VALUE);
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
        if (!isValid()) {
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
        return buffer.getInt(keySizeOffset());
    }

    private int keySizeOffset() {
        return magic() == MAGIC_VALUE_V0 ? KEY_OFFSET_V0 : KEY_OFFSET_V1;
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
        return magic() == MAGIC_VALUE_V0 ? KEY_OFFSET_V0 + Math.max(0, keySize()) : KEY_OFFSET_V1 + Math.max(0, keySize());
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
        return CompressionCodec.fromCodec(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
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
        return sliceDelimited(keySizeOffset());
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

    /**
     * The timestamp of the message, only available when the "magic" value is greater than 0
     * When magic > 0, The timestamp of a message is determined in the following way:
     * 1. wrapperMessageTimestampType = None and wrapperMessageTimestamp is None - Uncompressed message, timestamp and timestamp type are in the message.
     * 2. wrapperMessageTimestampType = LogAppendTime and wrapperMessageTimestamp is defined - Compressed message using LogAppendTime
     * 3. wrapperMessageTimestampType = CreateTime and wrapperMessageTimestamp is defined - Compressed message using CreateTime
     */

    public Long timestamp() {
        if (magic() == MAGIC_VALUE_V0) {
            return Message.NO_TIMESTAMP;
        }
        if (wrapperMessageTimestampType.isPresent() && wrapperMessageTimestampType.get() == TimestampType.LOG_APPEND_TIME && wrapperMessageTimestamp.isPresent()) {
            return wrapperMessageTimestamp.get();
        } else {
            return buffer.getLong(Message.TIMESTAMP_OFFSET);
        }
    }

    public TimestampType timestampType() {
        if (magic() == MAGIC_VALUE_V0) {
            return TimestampType.NO_TIMESTAMP_TYPE;
        } else {
            return wrapperMessageTimestampType.isPresent() ? wrapperMessageTimestampType.get() : TimestampType.forAttributes(attributes());
        }

    }


    public void convertToBuffer(Byte toMagicValue, ByteBuffer byteBuffer, Long now) {
        TimestampType timestampType = timestampType();
        if (byteBuffer.remaining() < size() + headerSizeDiff(magic(), toMagicValue)) {
            throw new IndexOutOfBoundsException(String.format("The byte buffer does not have enough capacity to hold new message format %s version", toMagicValue));
        }
        if (toMagicValue == Message.MAGIC_VALUE_V1) {
            // Up-conversion, reserve CRC and update magic byte
            byteBuffer.position(Message.MAGIC_OFFSET);
            byteBuffer.put(Message.MAGIC_VALUE_V1);
            byteBuffer.put(timestampType.updateAttributes(attributes()));
            // Up-conversion, insert the timestamp field
            if (timestampType == TimestampType.LOG_APPEND_TIME)
                byteBuffer.putLong(now);
            else
                byteBuffer.putLong(Message.NO_TIMESTAMP);
            byteBuffer.put(buffer.array(), buffer.arrayOffset() + Message.KEY_SIZE_OFFSET_V0, size() - Message.KEY_SIZE_OFFSET_V0);
        } else {
            // Down-conversion, reserve CRC and update magic byte
            byteBuffer.position(Message.MAGIC_OFFSET);
            byteBuffer.put(Message.MAGIC_VALUE_V0);
            byteBuffer.put(TimestampType.CREATE_TIME.updateAttributes(attributes()));
            // Down-conversion, skip the timestamp field
            byteBuffer.put(buffer.array(), buffer.arrayOffset() + Message.KEY_OFFSET_V1, size() - Message.KEY_OFFSET_V1);
        }
        // update crc value
        Message newMessage = new Message(byteBuffer);
        Utils.writeUnsignedInt(byteBuffer, Message.CRC_OFFSET, newMessage.computeChecksum());
        byteBuffer.rewind();
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
