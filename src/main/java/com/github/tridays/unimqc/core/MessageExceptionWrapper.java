package com.github.tridays.unimqc.core;

/**
 * @author xp
 */
public class MessageExceptionWrapper<T> {

    private final T message;

    private final Throwable cause;

    public MessageExceptionWrapper(T message, Throwable cause) {
        this.message = message;
        this.cause = cause;
    }

    public T getMessage() {
        return message;
    }

    public Throwable getCause() {
        return cause;
    }
}
