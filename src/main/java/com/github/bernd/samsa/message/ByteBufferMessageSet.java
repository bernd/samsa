package com.github.bernd.samsa.message;

import com.github.bernd.samsa.compression.CompressionCodec;
import com.github.bernd.samsa.compression.CompressionFactory;
import com.github.bernd.samsa.utils.IteratorTemplate;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A sequence of messages stored in a byte buffer
 * <p/>
 * There are two ways to create a ByteBufferMessageSet
 * <p/>
 * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
 * <p/>
 * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
 */
public class ByteBufferMessageSet extends MessageSet {
    private final ByteBuffer buffer;

    private int shallowValidByteCount = -1;

    public ByteBufferMessageSet(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBufferMessageSet(final CompressionCodec compressionCodec,
                                final List<Message> messages) throws IOException {
        this(create(new AtomicLong(0), compressionCodec, messages));
    }

    public ByteBufferMessageSet(final CompressionCodec compressionCodec,
                                final AtomicLong offsetCounter,
                                final List<Message> messages) throws IOException {
        this(create(offsetCounter, compressionCodec, messages));
    }

    public ByteBufferMessageSet(final List<Message> messages) throws IOException {
        this(CompressionCodec.NONE, new AtomicLong(0), messages);
    }

    private static ByteBuffer create(final AtomicLong offsetCounter,
                                     final CompressionCodec compressionCodec,
                                     final List<Message> messages) throws IOException {
        if (messages.size() == 0) {
            return MessageSet.EMPTY.getBuffer();
        } else if (compressionCodec == CompressionCodec.NONE) {
            final ByteBuffer buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));

            for (Message message : messages) {
                writeMessage(buffer, message, offsetCounter.getAndIncrement());
            }

            buffer.rewind();
            return buffer;
        } else {
            final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream(MessageSet.messageSetSize(messages));
            final DataOutputStream output = new DataOutputStream(CompressionFactory.create(compressionCodec, byteArrayStream));
            long offset = -1L;
            try {
                for (Message message : messages) {
                    offset = offsetCounter.getAndIncrement();
                    output.writeLong(offset);
                    output.writeInt(message.size());
                    output.write(message.buffer().array(), message.buffer().arrayOffset(), message.buffer().limit());
                }
            } finally {
                output.close();
            }
            byte[] bytes = byteArrayStream.toByteArray();
            final Message message = new Message(bytes, compressionCodec);
            final ByteBuffer buffer = ByteBuffer.allocate(message.size() + MessageSet.LOG_OVERHEAD);
            writeMessage(buffer, message, offset);
            buffer.rewind();
            return buffer;
        }
    }

    public static ByteBufferMessageSet decompress(final Message message) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final InputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        byte[] intermediateBuffer = new byte[1024];
        final InputStream compressed = CompressionFactory.create(message.compressionCodec(), inputStream);
        try {
            while (true) {
                final int dataRead = compressed.read(intermediateBuffer);

                if (dataRead < 1) {
                    break;
                }

                outputStream.write(intermediateBuffer, 0, dataRead);
            }
        } finally {
            compressed.close();
        }
        final ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();

        return new ByteBufferMessageSet(outputBuffer);
    }

    public static void writeMessage(final ByteBuffer buffer, final Message message, final long offset) {
        buffer.putLong(offset);
        buffer.putInt(message.size());
        buffer.put(message.buffer());
        message.buffer().rewind();
    }

    private int shallowValidBytes() {
        if (shallowValidByteCount < 0) {
            int bytes = 0;
            final Iterator<MessageAndOffset> iter = internalIterator(true);
            while (iter.hasNext()) {
                final MessageAndOffset messageAndOffset = iter.next();
                bytes += MessageSet.entrySize(messageAndOffset.getMessage());
            }
            shallowValidByteCount = bytes;
        }
        return shallowValidByteCount;
    }

    /**
     * Write the messages in this set to the given channel
     */
    @Override
    public int writeTo(GatheringByteChannel channel, long offset, int maxSize) {
        // Ignore offset and size from input. We just want to write the whole buffer to the channel.
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes()) {
            try {
                written += channel.write(buffer);
            } catch (IOException e) {
                // TODO Handle exception?
                e.printStackTrace();
            }
        }
        buffer.reset();
        return written;
    }

    /**
     * default iterator that iterates over decompressed messages
     */
    @Override
    public Iterator<MessageAndOffset> iterator() {
        return internalIterator(false);
    }

    /**
     * iterator over compressed messages without decompressing
     */
    public Iterator<MessageAndOffset> shallowIterator() {
        return internalIterator(true);
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    private Iterator<MessageAndOffset> internalIterator(final boolean isShallow) {
        return new IteratorTemplate<MessageAndOffset>() {
            private ByteBuffer topIter = buffer.slice();
            private Iterator<MessageAndOffset> innerIter = null;

            @Override
            protected MessageAndOffset makeNext() {
                if (isShallow) {
                    return makeNextOuter();
                } else {
                    if (innerDone()) {
                        return makeNextOuter();
                    } else {
                        return innerIter.next();
                    }
                }
            }

            public boolean innerDone() {
                return innerIter == null || !innerIter.hasNext();
            }

            public MessageAndOffset makeNextOuter() {
                // if there isn't at least an offset and size, we are done
                if (topIter.remaining() < 12) {
                    return allDone();
                }
                long offset = topIter.getLong();
                int size = topIter.getInt();
                if (size < Message.MIN_HEADER_SIZE) {
                    throw new InvalidMessageException("Message found with corrupt size (" + size + ")");
                }

                // we have an incomplete message
                if (topIter.remaining() < size) {
                    return allDone();
                }

                // read the current message and check correctness
                ByteBuffer message = topIter.slice();
                message.limit(size);
                topIter.position(topIter.position() + size);
                final Message newMessage = new Message(message.array());

                if (isShallow) {
                    return new MessageAndOffset(newMessage, offset);
                } else {
                    switch (newMessage.compressionCodec()) {
                        case NONE:
                            innerIter = null;
                            return new MessageAndOffset(newMessage, offset);
                        default:
                            try {
                                innerIter = ByteBufferMessageSet.decompress(newMessage).internalIterator(false);
                            } catch (IOException e) {
                                // TODO Ignore?
                                e.printStackTrace();
                            }
                            if (!innerIter.hasNext()) {
                                innerIter = null;
                            }
                            return makeNext();
                    }
                }
            }
        };
    }

    /**
     * Update the offsets for this message set. This method attempts to do an in-place conversion
     * if there is no compression, but otherwise recopies the messages
     */
    public ByteBufferMessageSet assignOffsets(final AtomicLong offsetCounter, final CompressionCodec codec) throws IOException {
        if (codec == CompressionCodec.NONE) {
            // do an in-place conversion
            int position = 0;
            buffer.mark();
            while (position < sizeInBytes() - MessageSet.LOG_OVERHEAD) {
                buffer.position(position);
                buffer.putLong(offsetCounter.getAndIncrement());
                position += MessageSet.LOG_OVERHEAD + buffer.getInt();
            }
            buffer.reset();
            return this;
        } else {
            // messages are compressed, crack open the messageset and recompress with correct offset
            final Iterator<Message> messages = Iterators.transform(internalIterator(false), new Function<MessageAndOffset, Message>() {
                @Override
                public Message apply(MessageAndOffset messageAndOffset) {
                    return messageAndOffset.getMessage();
                }
            });
            return new ByteBufferMessageSet(codec, offsetCounter, Lists.newArrayList(messages));
        }
    }
}
