package com.github.tridays.unimqc.mns;

import java.util.function.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.github.tridays.unimqc.MQClient;
import com.github.tridays.unimqc.Codec;
import com.github.tridays.unimqc.utils.JDKCodec;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AliyunMNSClient<T> implements MQClient<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunMNSClient.class);

    private final MNSClient mnsClient;

    private final String queueName;

    @SuppressWarnings("unchecked")
    private final Codec<T, byte[]> codec = JDKCodec.INSTANCE;

    private boolean initialized = false;

    private CloudQueue queue;

    @Getter
    private AliyunMNSSender<T> sender;

    @Override
    public void init() throws Exception {
        queue = mnsClient.getQueueRef(queueName);
        initialized = true;
        sender = new AliyunMNSSender<>(queue, codec);
    }

    @Override
    public void send(T t) {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        sender.send(t);
    }

    public AliyunMNSListener<T> listen(int receiveTimeout, int receiveMaxCount, boolean emptyMessageCallback) {
        return listen(() -> new AliyunMNSListener<>(queue, codec), receiveTimeout, receiveMaxCount, emptyMessageCallback);
    }

    public <R extends AliyunMNSListener<T>> R listen(Supplier<R> supplier, int receiveTimeout, int receiveMaxCount, boolean emptyMessageCallback) {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        R listener = supplier.get();
        listener.setReceiveTimeout(receiveTimeout);
        listener.setReceiveMaxCount(receiveMaxCount);
        listener.setEmptyMessageCallback(emptyMessageCallback);
        return listener;
    }

}
