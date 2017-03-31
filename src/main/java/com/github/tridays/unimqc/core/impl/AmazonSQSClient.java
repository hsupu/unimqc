package com.github.tridays.unimqc.core.impl;

import java.util.function.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.github.tridays.unimqc.MQClient;
import com.github.tridays.unimqc.core.Codec;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AmazonSQSClient<T> implements MQClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonSQSClient.class);

    private final AmazonSQS amazonSQS;

    private final String queueName;

    @SuppressWarnings("unchecked")
    private final Codec<T, byte[]> codec = JDKCodec.INSTANCE;

    private boolean initialized = false;

    private String queueUrl;

    @Getter
    private AmazonSQSSender<T> sender;

    @Override
    public void init() throws Exception {
        GetQueueUrlResult result = amazonSQS.getQueueUrl(queueName);
        queueUrl = result.getQueueUrl();
        initialized = true;
        sender = new AmazonSQSSender<>(amazonSQS, queueUrl, codec);
    }

    @Override
    public void send(T t) {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        sender.send(t);
    }

    public AmazonSQSListener<T> listen(int receiveTimeout, int receiveMaxCount, int visibilityTimeout, boolean emptyMessageCallback) {
        return listen(() -> new AmazonSQSListener<>(amazonSQS, queueUrl, codec), receiveTimeout, receiveMaxCount, visibilityTimeout, emptyMessageCallback);
    }

    public <R extends AmazonSQSListener<T>> R listen(Supplier<R> supplier, int receiveTimeout, int receiveMaxCount, int visibilityTimeout, boolean emptyMessageCallback) {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        R listener = supplier.get();
        listener.setReceiveTimeout(receiveTimeout);
        listener.setReceiveMaxCount(receiveMaxCount);
        listener.setVisibilityTimeout(visibilityTimeout);
        listener.setEmptyMessageCallback(emptyMessageCallback);
        return listener;
    }
}
