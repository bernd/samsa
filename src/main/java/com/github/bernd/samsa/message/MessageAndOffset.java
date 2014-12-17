package com.github.bernd.samsa.message;

public class MessageAndOffset {
    private final Message message;
    private final long offset;

    public MessageAndOffset(final Message message, final long offset) {
        this.message = message;
        this.offset = offset;
    }

    public long nextOffset() {
        return offset + 1;
    }

    public Message getMessage() {
        return message;
    }

    public long getOffset() {
        return offset;
    }
}
