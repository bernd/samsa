package com.github.bernd.samsa.message;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.utils.Utils;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MessageTest {
    public static class MessageTestVal {
        public final byte[] key;
        public final byte[] payload;
        public final CompressionCodec codec;
        public final Message message;

        public MessageTestVal(final byte[] key,
                                     final byte[] payload,
                                     final CompressionCodec codec,
                                     final Message message) {
            this.key = key;
            this.payload = payload;
            this.codec = codec;
            this.message = message;
        }

        @Override
        public String toString() {
            return "MessageTestVal{" +
                    "key=" + Arrays.toString(key) +
                    ", payload=" + Arrays.toString(payload) +
                    ", codec=" + codec +
                    ", message=" + message +
                    '}';
        }
    }

    private List<byte[]> keys;
    private List<byte[]> vals;
    private List<CompressionCodec> codecs;
    private List<MessageTestVal> messages;

    @BeforeMethod
    public void setUp() throws Exception {
        messages = Lists.newArrayList();

        keys = Lists.newArrayList(null, "key".getBytes(), "".getBytes());
        vals = Lists.newArrayList("value".getBytes(), "".getBytes(), null);
        //codecs = Lists.newArrayList(CompressionCodec.NONE, CompressionCodec.GZIP,
        //        CompressionCodec.SNAPPY, CompressionCodec.LZ4);
        codecs = Lists.newArrayList(CompressionCodec.NONE);

        for (final byte[] key : keys) {
            for (final byte[] val : vals) {
                for (final CompressionCodec codec : codecs) {
                    messages.add(new MessageTestVal(key, val, codec, new Message(val, key, codec)));
                }
            }
        }
    }

    @Test
    public void testFieldValues() throws Exception {
        for (final MessageTestVal val : messages) {
            if(val.payload == null) {
                assertTrue(val.message.isNull());
                assertEquals(val.message.payload(), null, "Payload should be null");
            } else {
                TestUtils.checkEquals(val.message.payload(), ByteBuffer.wrap(val.payload));
            }
            assertEquals(val.message.magic(), Message.CURRENT_MAGIC_VALUE);
            if(val.message.hasKey()) {
                TestUtils.checkEquals(val.message.key(), ByteBuffer.wrap(val.key));
            } else {
                assertEquals(val.message.key(), null);
            }
            assertEquals(val.message.compressionCodec(), val.codec);
        }

    }

    @Test
    public void testChecksum() throws Exception {
        for (final MessageTestVal val : messages) {
            assertTrue(val.message.isValid(), "Auto-computed checksum should be valid");
            // garble checksum
            final int badChecksum = (int) (val.message.checksum() + 1 % Integer.MAX_VALUE);
            Utils.writeUnsignedInt(val.message.buffer(), Message.CRC_OFFSET, badChecksum);
            assertFalse(val.message.isValid(), "Message with invalid checksum should be invalid");
        }
    }

    @Test
    public void testEquality() throws Exception {
        for(final MessageTestVal val : messages) {
            assertFalse(val.message.equals(null), "Should not equal null");
            assertFalse(val.message.equals("asdf"), "Should not equal a random string");
            assertTrue(val.message.equals(val.message), "Should equal itself");
            final Message copy = new Message(val.payload, val.key, val.codec);
            assertTrue(val.message.equals(copy), "Should equal another message with the same content.");
        }
    }

    @Test
    public void testIsHashable() throws Exception {
        // this is silly, but why not
        final HashMap<Message, Message> m = new HashMap<>();
        for(final MessageTestVal val : messages) {
            m.put(val.message, val.message);
        }
        for(final MessageTestVal val : messages) {
            assertEquals(val.message, m.get(val.message));
        }
    }
}
