package com.github.bernd.samsa.message;

import com.github.bernd.samsa.compression.CompressionCodec;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.util.List;

public class ByteBufferMessageSetTest extends BaseMessageSetTestCases {

    @BeforeMethod
    public void setUp() throws Exception {

    }

    @Override
    public MessageSet createMessageSet(List<Message> messages, CompressionCodec compressed) throws IOException {
        return new ByteBufferMessageSet(compressed, messages);
    }

    @Override
    public MessageSet createMessageSet(List<Message> messages) throws IOException {
        return createMessageSet(messages, CompressionCodec.NONE);
    }
}