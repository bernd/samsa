package com.github.bernd.samsa.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A base class that simplifies implementing an iterator
 *
 * @param <T> The type of thing we are iterating over
 */
public abstract class AbstractIterator<T> implements Iterator<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractIterator.class);

    private enum State {
        DONE, READY, NOT_READY, FAILED
    }

    private State state = State.NOT_READY;
    private T nextItem = null;

    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        if (nextItem == null) {
            throw new IllegalStateException("Expected item but none found.");
        }
        return nextItem;
    }

    public T peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextItem;
    }

    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                return false;
            case READY:
                return true;
            default:
                return maybeComputeNext();
        }
    }

    protected abstract T makeNext();

    private boolean maybeComputeNext() {
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
}
