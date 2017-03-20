package com.github.tridays.unimqc.core.impl;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.github.tridays.unimqc.MQClient;
import com.github.tridays.unimqc.core.Codec;
import com.github.tridays.unimqc.core.MessageExceptionWrapper;

import lombok.RequiredArgsConstructor;

import com.github.tridays.unimqc.core.MQListener;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AmazonSQSClient<T> implements MQClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonSQSClient.class);

    private final AmazonSQS amazonSQS;

    private final String queueName;

    private final Codec<T, String> codec;

    private String queueUrl;

    @Override
    public void init() throws Exception {
        GetQueueUrlResult result = amazonSQS.getQueueUrl(queueName);
        queueUrl = result.getQueueUrl();
    }

    protected void doSend(String s) {
        SendMessageRequest request = new SendMessageRequest(queueUrl, s);
        try {
            SendMessageResult result = amazonSQS.sendMessage(request);
            if (LOG.isTraceEnabled()) {
                LOG.trace("SQS send message " + result.getMessageId());
            }
        } catch (AmazonServiceException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("SQS server error: " + e.getMessage(), e);
            }
            throw e;
        } catch (AmazonClientException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("SQS client error: " + e.getMessage(), e);
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
        private final int visibilityTimeout;
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
            ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
            request.setWaitTimeSeconds(receiveTimeout);
            request.setMaxNumberOfMessages(receiveMaxCount);
            request.setVisibilityTimeout(visibilityTimeout);
            try {
                ReceiveMessageResult result = amazonSQS.receiveMessage(request);
                if (result.getMessages() == null) {
                    return Collections.emptyList();
                }
                return result.getMessages();
            } catch (AmazonServiceException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("SQS server error: " + e.getMessage(), e);
                }
                throw e;
            } catch (AmazonClientException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("SQS client error: " + e.getMessage(), e);
                }
                throw e;
            }
        }

        protected boolean handleReceiveError(Throwable e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("SQS receive error: " + e.getMessage(), e);
            }
            return true;
        }

        protected void consume(Message message) {
            if (message == null) {
                callback.accept(null);
            } else {
                callback.accept(codec.decode(message.getBody()));
            }
        }

        protected boolean handleConsumeError(MessageExceptionWrapper<Message> ew) {
            if (LOG.isWarnEnabled()) {
                if (ew.getMessage() == null) {
                    LOG.warn("SQS consume error: " + ew.getCause().getMessage(), ew.getCause());
                } else {
                    LOG.warn("SQS consume error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
                }
            }
            return false;
        }

        protected void acknowledge(Message message) {
            try {
                DeleteMessageRequest request = new DeleteMessageRequest(queueUrl, message.getReceiptHandle());
                amazonSQS.deleteMessage(request);
            } catch (AmazonServiceException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("SQS server error: " + e.getMessage(), e);
                }
                throw e;
            } catch (AmazonClientException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("SQS client error: " + e.getMessage(), e);
                }
                throw e;
            }
        }

        protected boolean handleAcknowledgeError(MessageExceptionWrapper<Message> ew) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("SQS ack error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
            }
            return false;
        }
    }

}
