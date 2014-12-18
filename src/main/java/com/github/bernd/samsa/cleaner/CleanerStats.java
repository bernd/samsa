package com.github.bernd.samsa.cleaner;

import com.github.bernd.samsa.utils.SamsaTime;

/**
 * A simple struct for collecting stats about log cleaning.
 * <p/>
 * This class it not thread-safe!
 */
public class CleanerStats {
    private final SamsaTime time;

    public long startTime = 0L;
    public long mapCompleteTime = 0L;
    public long endTime = 0L;
    public long bytesRead = 0L;
    public long bytesWritten = 0L;
    public long mapBytesRead = 0L;
    public long mapMessagesRead = 0L;
    public long messagesRead = 0L;
    public long messagesWritten = 0L;
    public double bufferUtilization = 0.0d;

    public CleanerStats(final SamsaTime time) {
        this.time = time;

        clear();
    }

    public void readMessage(final int size) {
        messagesRead += 1;
        bytesRead += size;
    }

    public void recopyMessage(final int size) {
        messagesWritten += 1;
        bytesWritten += size;
    }

    public void indexMessage(final int size) {
        mapMessagesRead += 1;
        mapBytesRead += size;
    }

    public void indexDone() {
        mapCompleteTime = time.milliseconds();
    }

    public void allDone() {
        endTime = time.milliseconds();
    }

    public double elapsedSecs() {
        return (endTime - startTime) / 1000.0;
    }

    public double elapsedIndexSecs() {
        return (mapCompleteTime - startTime) / 1000.0;
    }

    public void clear() {
        startTime = time.milliseconds();
        mapCompleteTime = -1L;
        endTime = -1L;
        bytesRead = 0L;
        bytesWritten = 0L;
        mapBytesRead = 0L;
        mapMessagesRead = 0L;
        messagesRead = 0L;
        messagesWritten = 0L;
        bufferUtilization = 0.0d;
    }
}
