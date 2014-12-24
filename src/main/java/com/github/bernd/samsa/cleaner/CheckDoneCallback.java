package com.github.bernd.samsa.cleaner;

public interface CheckDoneCallback<T> {
    public void call(T argument) throws LogCleaningAbortedException;
}
