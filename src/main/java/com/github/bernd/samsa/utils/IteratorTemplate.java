package com.github.bernd.samsa.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class IteratorTemplate<T> implements Iterator<T> {
    private enum State {
        DONE, READY, NOT_READY, FAILED;
    }

    private State state = State.NOT_READY;
    private T nextItem = null;

    public T next() {
        if (! hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        if (nextItem == null) {
            throw new IllegalStateException("Expected item but none found.");
        }
        return nextItem;
    }

    public T peek() {
        if (! hasNext()) {
            throw new NoSuchElementException();
        }
        return nextItem;
    }

    public boolean hasNext() {
        if (state == State.FAILED) {
            throw new IllegalStateException("Iterator is in failed state");
        }
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            default:
                try {
                    return maybeComputeNext();
                } catch (IOException e) {
                    // TODO What to do with the exception here?
                    e.printStackTrace();
                    return false;
                }
        }
    }

    protected abstract T makeNext() throws IOException;

    private boolean maybeComputeNext() throws IOException {
        state = State.FAILED;
        nextItem = makeNext();
        if (state == State.DONE) {
            return false;
        } else {
            state = State.READY;
            return true;
        }
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }

    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    protected void resetState() {
        state = State.NOT_READY;
    }
}
