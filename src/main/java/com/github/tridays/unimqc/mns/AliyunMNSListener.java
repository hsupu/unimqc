package com.github.tridays.unimqc.mns;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Message;
import com.github.tridays.unimqc.Codec;
import com.github.tridays.unimqc.MQListener;
import com.github.tridays.unimqc.MessageExceptionWrapper;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AliyunMNSListener<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunMNSListener.class);

    private final CloudQueue queue;

    private final Codec<T, byte[]> codec;

    @Setter
    private int receiveTimeout = 0;

    @Setter
    private int receiveMaxCount = 1;

    @Setter
    private boolean emptyMessageCallback = false;

    @Setter
    private Predicate<Throwable> receiveErrorHandler;

    @Setter
    private Predicate<MessageExceptionWrapper<Message>> consumeErrorHandler;

    @Setter
    private Predicate<MessageExceptionWrapper<Message>> acknowledgeErrorHandler;

    private Consumer<Message> consume;

    public MQListener<Message> listen(final Consumer<T> callback) throws Exception {
        this.consume = (message) -> {
            if (message == null) {
                callback.accept(null);
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("consume message " + message.getMessageId());
                }
                callback.accept(codec.decode(message.getMessageBodyAsBytes()));
            }
        };
        return build();
    }

    public MQListener<Message> listenRaw(final Consumer<Message> rawCallback) throws Exception {
        this.consume = (message) -> {
            if (message == null) {
                rawCallback.accept(null);
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("consume message " + message.getMessageId());
                }
                rawCallback.accept(message);
            }
        };
        return build();
    }

    private MQListener<Message> build() throws Exception {
        ThreadGroup threadGroup = new ThreadGroup("mnsListener");
        return new MQListener<>(
                Executors.newCachedThreadPool(runnable -> new Thread(threadGroup, runnable)),
                this::receive,
                this::handleReceiveError,
                consume,
                emptyMessageCallback,
                this::handleConsumeError,
                this::acknowledge,
                this::handleAcknowledgeError);
    }

    private List<Message> receive() throws ClientException, ServiceException {
        List<Message> messages = queue.batchPopMessage(receiveMaxCount, receiveTimeout);
        if (messages == null) {
            return Collections.emptyList();
        }
        return messages;
    }

    protected boolean handleReceiveError0(Throwable e) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("receive error: " + e.getMessage(), e);
        }
        return true;
    }

    private boolean handleReceiveError(Throwable e) {
        if (receiveErrorHandler == null) {
            return handleReceiveError0(e);
        } else {
            return receiveErrorHandler.test(e);
        }
    }

    protected boolean handleConsumeError0(MessageExceptionWrapper<Message> ew) {
        if (LOG.isWarnEnabled()) {
            if (ew.getMessage() == null) {
                LOG.warn("consume error: " + ew.getCause().getMessage(), ew.getCause());
            } else {
                LOG.warn("consume error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
            }
        }
        return false;
    }

    private boolean handleConsumeError(MessageExceptionWrapper<Message> ew) {
        if (consumeErrorHandler == null) {
            return handleConsumeError0(ew);
        } else {
            return consumeErrorHandler.test(ew);
        }
    }

    private void acknowledge(Message message) throws ClientException, ServiceException {
        queue.deleteMessage(message.getReceiptHandle());
    }

    protected boolean handleAcknowledgeError0(MessageExceptionWrapper<Message> ew) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("ack error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
        }
        return false;
    }

    private boolean handleAcknowledgeError(MessageExceptionWrapper<Message> ew) {
        if (acknowledgeErrorHandler == null) {
            return handleAcknowledgeError0(ew);
        } else {
            return acknowledgeErrorHandler.test(ew);
        }
    }
}
