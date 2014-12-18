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

    @Override
    public String toString() {
        return "MessageAndOffset{" +
                "message=" + message +
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageAndOffset that = (MessageAndOffset) o;

        if (offset != that.offset) return false;
        if (!message.equals(that.message)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = message.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

}
