package com.github.bernd.samsa.cleaner;

import com.github.bernd.samsa.cleaner.LogCleaningAbortedException;

public interface CheckDoneCallback<T> {
    public void call(T argument) throws LogCleaningAbortedException;
}
