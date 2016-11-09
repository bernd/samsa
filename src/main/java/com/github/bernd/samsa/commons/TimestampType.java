package com.github.bernd.samsa.commons;

import com.github.bernd.samsa.message.Message;

import java.util.NoSuchElementException;

/**
 * The timestamp type of the records.
 */
public enum TimestampType {
    NO_TIMESTAMP_TYPE(-1, "NoTimestampType"), CREATE_TIME(0, "CreateTime"), LOG_APPEND_TIME(1, "LogAppendTime");

    public final int id;
    public final String name;
    TimestampType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte updateAttributes(byte attributes) {
        return this == CREATE_TIME ?
            (byte) (attributes & ~Message.TIMESTAMP_TYPE_MASK) : (byte) (attributes | Message.TIMESTAMP_TYPE_MASK);
    }

    public byte updateAttributes(int attributes) {
       return updateAttributes((byte)attributes);
    }


    public static TimestampType forAttributes(byte attributes) {
        int timestampType = (attributes & Message.TIMESTAMP_TYPE_MASK) >> Message.TIMESTAMP_TYPE_ATTRIBUTEBIT_OFFSET;
        return timestampType == 0 ? CREATE_TIME : LOG_APPEND_TIME;
    }

    public static TimestampType forName(String name) {
        for (TimestampType t : values())
            if (t.name.equals(name))
                return t;
        throw new NoSuchElementException("Invalid timestamp type " + name);
    }

    @Override
    public String toString() {
        return name;
    }
}
