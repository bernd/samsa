package com.github.bernd.samsa.message;

import com.github.bernd.samsa.OffsetPosition;
import com.github.bernd.samsa.TestUtils;
import com.github.bernd.samsa.compression.CompressionCodec;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileMessageSetTest extends BaseMessageSetTestCases {
    private FileMessageSet messageSet;

    @Override
    public MessageSet createMessageSet(List<Message> messages, CompressionCodec compressed) throws IOException {
        final FileMessageSet set = new FileMessageSet(TestUtils.tempFile());
        set.append(new ByteBufferMessageSet(compressed, messages));
        set.flush();
        return set;
    }

    @Override
    public MessageSet createMessageSet(List<Message> messages) throws IOException {
        return createMessageSet(messages, CompressionCodec.NONE);
    }

    @BeforeEach
    public void setUp() throws Exception {
        messageSet = (FileMessageSet) createMessageSet(messages);
    }

    /**
     * Test that the cached size variable matches the actual file size as we append messages
     */
    @Test
    public void testFileSize() throws Exception {
        assertEquals(messageSet.sizeInBytes(), messageSet.getChannel().size());
        for (int i = 0; i < 20; i++) {
            messageSet.append(TestUtils.singleMessageSet("abcd".getBytes()));
            assertEquals(messageSet.sizeInBytes(), messageSet.getChannel().size());
        }
    }

    /**
     * Test that adding invalid bytes to the end of the log doesn't break iteration
     */
    @Test
    public void testIterationOverPartialAndTruncation() throws Exception {
        testPartialWrite(0, messageSet);
        testPartialWrite(2, messageSet);
        testPartialWrite(4, messageSet);
        testPartialWrite(5, messageSet);
        testPartialWrite(6, messageSet);
    }

    private void testPartialWrite(final int size, final FileMessageSet messageSet) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        final long originalPosition = messageSet.getChannel().position();
        for (int i = 0; i < size; i++) {
            buffer.put((byte) 0);
        }
        buffer.rewind();
        messageSet.getChannel().write(buffer);
        // appending those bytes should not change the contents
        TestUtils.checkEquals(messages.iterator(), messagesFromMessageSet(messageSet).iterator());
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     */
    @Test
    public void testIterationDoesntChangePosition() throws Exception {
        final long position = messageSet.getChannel().position();
        TestUtils.checkEquals(messages.iterator(), messagesFromMessageSet(messageSet).iterator());
        assertEquals(messageSet.getChannel().position(), position);
    }

    private List<Message> messagesFromMessageSet(final FileMessageSet messageSet) {
        final List<Message> messages = Lists.newArrayList();

        for (final MessageAndOffset messageAndOffset : messageSet) {
            messages.add(messageAndOffset.getMessage());
        }

        return messages;
    }

    /**
     * Test a simple append and read.
     */
    @Test
    public void testRead() throws Exception {
        FileMessageSet read = messageSet.read(0, messageSet.sizeInBytes());
        TestUtils.checkEquals(messageSet.iterator(), read.iterator());

        final List<MessageAndOffset> items = Lists.newArrayList(read.iterator());
        final MessageAndOffset sec = items.get(1);

        read = messageSet.read(MessageSet.entrySize(sec.getMessage()), messageSet.sizeInBytes());
        assertEquals(Lists.newArrayList(read), items.subList(1, items.size()), "Try a read starting from the second message");

        read = messageSet.read(MessageSet.entrySize(sec.getMessage()), MessageSet.entrySize(sec.getMessage()));
        assertEquals(Lists.newArrayList(read), Lists.newArrayList(items.get(1)), "Try a read of a single message starting from the second message");
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    public void testSearch() throws Exception {
        // append a new message with a high offset
        final Message lastMessage = new Message("test".getBytes());

        messageSet.append(new ByteBufferMessageSet(CompressionCodec.NONE, new AtomicLong(50), Lists.newArrayList(lastMessage)));

        int position = 0;

        assertEquals(messageSet.searchFor(0, 0),
                new OffsetPosition(0L, position),
                "Should be able to find the first message by its offset");

        position += MessageSet.entrySize(Lists.newArrayList(messageSet).get(0).getMessage());

        assertEquals(messageSet.searchFor(1, 0),
                new OffsetPosition(1L, position),
                "Should be able to find second message when starting from 0");
        assertEquals(messageSet.searchFor(1, position),
                new OffsetPosition(1L, position),
                "Should be able to find second message starting from its offset");

        final List<MessageAndOffset> list = Lists.newArrayList(messageSet);

        position += MessageSet.entrySize(list.get(1).getMessage()) + MessageSet.entrySize(list.get(2).getMessage());

        assertEquals(messageSet.searchFor(3, position),
                new OffsetPosition(50L, position),
                "Should be able to find fourth message from a non-existant offset");
        assertEquals(messageSet.searchFor(50, position),
                new OffsetPosition(50L, position),
                "Should be able to find fourth message by correct offset");
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() throws Exception {
        final MessageAndOffset message = Lists.newArrayList(messageSet).get(1);
        final int start = messageSet.searchFor(1, 0).getPosition();
        final int size = message.getMessage().size();
        final FileMessageSet slice = messageSet.read(start, size);
        assertEquals(Lists.newArrayList(slice), Lists.newArrayList(message));
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    public void testTruncate() throws Exception {
        final MessageAndOffset message = Lists.newArrayList(messageSet).get(0);
        final int end = messageSet.searchFor(1, 0).getPosition();
        messageSet.truncateTo(end);
        assertEquals(Lists.newArrayList(messageSet), Lists.newArrayList(message));
        assertEquals(messageSet.sizeInBytes(), MessageSet.entrySize(message.getMessage()));
    }
}
