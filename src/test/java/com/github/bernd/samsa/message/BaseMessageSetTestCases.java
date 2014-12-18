package com.github.bernd.samsa.message;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.github.bernd.samsa.message.TestUtils.checkEquals;
import static org.testng.Assert.assertEquals;

public abstract class BaseMessageSetTestCases {
    final protected List<Message> messages = Lists.newArrayList(new Message("abcd".getBytes()), new Message("efgh".getBytes()));

    abstract public MessageSet createMessageSet(List<Message> messages, CompressionCodec compressed) throws IOException;

    abstract public MessageSet createMessageSet(List<Message> messages) throws IOException;

    public Iterator<Message> toMessageIterator(final MessageSet messageSet) {
        return Iterables.transform(messageSet, new Function<MessageAndOffset, Message>() {
            @Override
            public Message apply(MessageAndOffset messageAndOffset) {
                return messageAndOffset.getMessage();
            }
        }).iterator();
    }

    @Test
    public void testWrittenEqualsRead() throws Exception {
        checkEquals(messages.iterator(), toMessageIterator(createMessageSet(messages)));
    }

    @Test
    public void testIteratorIsConsistent() throws Exception {
        final MessageSet messageSet = createMessageSet(messages);
        // two iterators over the same set should give the same results
        checkEquals(messageSet.iterator(), messageSet.iterator());
    }

    @Test
    public void testIteratorIsConsistentWithCompression() throws Exception {
        final MessageSet m = createMessageSet(messages, CompressionCodec.NONE);
        // two iterators over the same set should give the same results
        checkEquals(m.iterator(), m.iterator());
    }

    @Test
    public void testSizeInBytes() throws Exception {
        assertEquals(createMessageSet(new ArrayList<Message>()).sizeInBytes(), 0,
                "Empty message set should have 0 bytes.");
        assertEquals(createMessageSet(messages).sizeInBytes(), MessageSet.messageSetSize(messages),
                "Predicted size should equal actual size.");
    }

    @Test
    public void testSizeInBytesWithCompression() throws Exception {
        assertEquals(createMessageSet(new ArrayList<Message>(), CompressionCodec.NONE).sizeInBytes(),
                0, // overhead of the GZIP output stream
                "Empty message set should have 0 bytes.");
    }

    @Test
    public void testWriteTo() throws Exception {
        // test empty message set
        testWriteToWithMessageSet(createMessageSet(new ArrayList<Message>()));
        testWriteToWithMessageSet(createMessageSet(messages));
    }

    public void testWriteToWithMessageSet(final MessageSet set) throws Exception {
        // do the write twice to ensure the message set is restored to its orginal state
        for (int i = 0; i < 2; i++) {
            final File file = File.createTempFile("message-set", "write-test");
            final FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            final int written = set.writeTo(channel, 0, 1024);
            assertEquals(set.sizeInBytes(), written, "Expect to write the number of bytes in the set.");
            final MessageSet newSet = new FileMessageSet(file, channel);
            checkEquals(set.iterator(), newSet.iterator());
        }
    }
}
