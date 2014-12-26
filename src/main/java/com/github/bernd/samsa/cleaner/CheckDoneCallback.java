package com.github.bernd.samsa.cleaner;

public interface CheckDoneCallback<T> {
    void call(T argument) throws LogCleaningAbortedException;
}
