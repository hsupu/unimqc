package com.github.tridays.unimqc.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author xp
 */
@RequiredArgsConstructor
public final class MQListener<T> implements Closeable {

    @Getter
    private final ExecutorService executorService;

    private final Supplier<? extends Collection<T>> receiver;

    // return true to continue, else to close
    private final Predicate<Throwable> receiveErrorHandler;

    private final Consumer<T> consumer;

    private final boolean emptyMessageCallback;

    // return true to continue(ack), false to skip and throw to close
    private final Predicate<MessageExceptionWrapper<T>> consumeErrorHandler;

    private final Consumer<T> acknowledgeHandler;

    // return true to continue, false to skip and throw to close
    private final Predicate<MessageExceptionWrapper<T>> acknowledgeErrorHandler;

    public enum Operation {
        NONE,
        START,
        STOP,
        CLOSE,
    }

    private volatile Operation operation = Operation.NONE;

    private final Object operationLock = new Object();

    public enum State {
        RUNNING,
        STOPPED,
        CLOSED,
    }

    @Getter
    private volatile int taskCount = 0;

    private Map<Object, ListenTask> tasks = new ConcurrentHashMap<>();

    private final Object taskLock = new Object();

    private Queue<T> messageBuffer = new ConcurrentLinkedDeque<>();

    private final Object bufferLock = new Object();

    public ListenTask incTask() {
        synchronized (this.operationLock) {
            Object stateLock = new Object();
            ListenTask listenTask = new ListenTaskImpl(stateLock);
            this.executorService.execute(listenTask.getRunnable());
            return listenTask;
        }
    }

    public boolean decTask() {
        synchronized (this.operationLock) {
            if (this.taskCount == 0) {
                return false;
            }
            final Object stateLock = this.tasks.keySet().iterator().next();
            final ListenTask listenTask = this.tasks.get(stateLock);
            synchronized (stateLock) {
                listenTask.close();
                stateLock.notifyAll();
                return true;
            }
        }
    }

    public void start() {
        synchronized (this.operationLock) {
            this.tasks.forEach((stateLock, listenTask) -> listenTask.start());
        }
    }

    public void stop() {
        synchronized (this.operationLock) {
            this.tasks.forEach((stateLock, listenTask) -> listenTask.start());
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this.operationLock) {
            this.tasks.forEach((stateLock, listenTask) -> listenTask.close());
        }
    }

    private void onTaskCreating(final Object taskOperationLock, final ListenTask listenTask) {
        synchronized (this.taskLock) {
            this.tasks.put(taskOperationLock, listenTask);
            this.taskCount++;
        }
    }

    private void onTaskClosing(final Object taskOperationLock) {
        synchronized (this.taskLock) {
            this.tasks.remove(taskOperationLock);
            this.taskCount--;
        }
    }

    private T receive() {
        if (messageBuffer.isEmpty()) {
            synchronized (this.bufferLock) {
                if (messageBuffer.isEmpty()) {
                    try {
                        Collection<T> messages = receiver.get();
                        if (messages == null) {
                            // receive no message
                            return null;
                        }
                        for (T message : messages) {
                            messageBuffer.offer(message);
                        }
                    } catch (Throwable e) {
                        if (receiveErrorHandler != null && receiveErrorHandler.test(e)) {
                            return null;
                        }
                        throw e;
                    }
                }
            }
        }
        return messageBuffer.poll();
    }

    private boolean handle(T message, Consumer<T> handler, Predicate<MessageExceptionWrapper<T>> errorHandler) {
        try {
            handler.accept(message);
            return true;
        } catch (Throwable e) {
            if (errorHandler != null) {
                MessageExceptionWrapper<T> exceptionWrapper = new MessageExceptionWrapper<>(message, e);
                return errorHandler.test(exceptionWrapper);
            }
            throw e;
        }
    }

    private boolean consume(T message) {
        return handle(message, consumer, consumeErrorHandler);
    }

    private boolean acknowledge(T message) {
        if (message == null) {
            // no message to ack
            return true;
        }
        return handle(message, acknowledgeHandler, acknowledgeErrorHandler);
    }

    public interface ListenTask {

        boolean start();

        boolean stop();

        boolean close();

        Runnable getRunnable();

    }

    private class ListenTaskImpl implements ListenTask, Runnable {

        private final Object operationLock;

        private volatile State state;

        private volatile Operation operation;

        private ListenTaskImpl(final Object operationLock) {
            this.operationLock = operationLock;
        }

        public boolean isIdle() {
            return this.operation == Operation.NONE;
        }

        public boolean isStopped() {
            return this.state == State.STOPPED;
        }

        public boolean isClosed() {
            return this.state == State.CLOSED;
        }

        public boolean isRunning() {
            return this.state == State.RUNNING;
        }

        private boolean testAndRun(final Supplier<Boolean> tester, final Runnable synchronizedRunner) {
            synchronized (this.operationLock) {
                if (!tester.get()) {
                    return false;
                }
                synchronizedRunner.run();
                this.operationLock.notifyAll();
                return true;
            }
        }

        @Override
        public boolean start() {
            return testAndRun(
                    () -> isIdle() && !isClosed(),
                    () -> this.operation = Operation.START);
        }

        @Override
        public boolean stop() {
            return testAndRun(
                    () -> isIdle() && !isClosed(),
                    () -> this.operation = Operation.STOP);
        }

        @Override
        public boolean close() {
            return testAndRun(
                    () -> isIdle() && !isClosed(),
                    () -> this.operation = Operation.CLOSE);
        }

        private void run0() {
            while (!isClosed()) {
                synchronized (this.operationLock) {
                    switch (this.operation) {
                        case NONE:
                            if (isStopped()) {
                                try {
                                    this.operationLock.wait();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    return;
                                }
                            }
                            break;
                        case START:
                            this.state = State.RUNNING;
                            this.operation = Operation.NONE;
                            continue;
                        case STOP:
                            this.state = State.STOPPED;
                            this.operation = Operation.NONE;
                            continue;
                        case CLOSE:
                            return;
                    }
                }
                if (isIdle() && isRunning()) {
                    try {
                        T message = receive();
                        if (message != null) {
                            if (consume(message)) {
                                acknowledge(message);
                            }
                        } else if (emptyMessageCallback) {
                            consume(null);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        return;
                    }
                }
            }
        }

        @Override
        public void run() {
            synchronized (this.operationLock) {
                onTaskCreating(this.operationLock, this);
                this.operation = Operation.NONE;
                this.state = State.STOPPED;
            }
            run0();
            synchronized (this.operationLock) {
                this.state = State.CLOSED;
                onTaskClosing(this.operationLock);
            }
        }

        @Override
        public Runnable getRunnable() {
            return this;
        }
    }

}
