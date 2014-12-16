package com.github.bernd.samsa;

import com.github.bernd.samsa.message.MessageSet;

public class FetchDataInfo {
    private final LogOffsetMetadata offsetMetadata;
    private final MessageSet messageSet;

    public FetchDataInfo(final LogOffsetMetadata offsetMetadata,
                         final MessageSet messageSet) {
        this.offsetMetadata = offsetMetadata;
        this.messageSet = messageSet;
    }

    public LogOffsetMetadata getOffsetMetadata() {
        return offsetMetadata;
    }

    public MessageSet getMessageSet() {
        return messageSet;
    }
}
