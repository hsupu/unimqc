package com.github.tridays.unimqc.core.impl;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.apache.http.ConnectionClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Message;
import com.github.tridays.unimqc.MQClient;
import com.github.tridays.unimqc.core.Codec;
import com.github.tridays.unimqc.core.MQListener;
import com.github.tridays.unimqc.core.MessageExceptionWrapper;

import lombok.RequiredArgsConstructor;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AliyunMNSClient<T> implements MQClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunMNSClient.class);

    private final MNSClient mnsClient;

    private final String queueName;

    private final Codec<T, byte[]> codec;

    private CloudQueue queue;

    @Override
    public void init() throws Exception {
        queue = mnsClient.getQueueRef(queueName);
    }

    protected void doSend(byte[] b) {
        try {
            Message message = new Message(b);
            queue.putMessage(message);
            if (LOG.isTraceEnabled()) {
                LOG.trace("MNS put message " + message.getMessageId());
            }
        } catch (ClientException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("MNS client error: " + e.getRequestId(), e);
            }
            throw e;
        } catch (ServiceException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("MNS server error: " + e.getRequestId(), e);
            }
            throw e;
        }
    }

    @Override
    public void send(T t) {
        doSend(codec.encode(t));
    }

    @RequiredArgsConstructor
    public class ListenerBuilder {

        private final int threadNumber;
        private final int receiveTimeout;
        private final int receiveMaxCount;
        private final boolean emptyMessageCallback;
        private final Consumer<T> callback;

        public MQListener<Message> build() throws Exception {
            return new MQListener<>(
                    Executors.newFixedThreadPool(threadNumber),
                    this::receive,
                    this::handleReceiveError,
                    this::consume,
                    emptyMessageCallback,
                    this::handleConsumeError,
                    this::acknowledge,
                    this::handleAcknowledgeError);
        }

        protected List<Message> receive() {
            try {
                List<Message> messages = queue.batchPopMessage(receiveMaxCount, receiveTimeout);
                if (messages == null) {
                    return Collections.emptyList();
                }
                return messages;
            } catch (ClientException e) {
                if (e.getCause() instanceof ConnectionClosedException) {
                    return null;
                }
                if (LOG.isWarnEnabled()) {
                    LOG.warn("MNS client error: " + e.getRequestId(), e);
                }
                throw e;
            } catch (ServiceException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("MNS server error: " + e.getRequestId(), e);
                }
                throw e;
            }
        }

        protected boolean handleReceiveError(Throwable e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("MNS receive error: " + e.getMessage(), e);
            }
            return true;
        }

        protected void consume(Message message) {
            if (message == null) {
                callback.accept(null);
            } else {
                callback.accept(codec.decode(message.getMessageBodyAsBytes()));
            }
        }

        protected boolean handleConsumeError(MessageExceptionWrapper<Message> ew) {
            if (LOG.isWarnEnabled()) {
                if (ew.getMessage() == null) {
                    LOG.warn("MNS consume error: " + ew.getCause().getMessage(), ew.getCause());
                } else {
                    LOG.warn("MNS consume error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
                }
            }
            return false;
        }

        protected void acknowledge(Message message) {
            try {
                queue.deleteMessage(message.getReceiptHandle());
            } catch (ClientException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("MNS client error: " + e.getRequestId(), e);
                }
                throw e;
            } catch (ServiceException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("MNS server error: " + e.getRequestId(), e);
                }
                throw e;
            }
        }

        protected boolean handleAcknowledgeError(MessageExceptionWrapper<Message> ew) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("MNS ack error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
            }
            return false;
        }
    }

}
