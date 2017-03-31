package com.github.tridays.unimqc.core.impl;

import java.util.function.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.model.Message;
import com.github.tridays.unimqc.core.Codec;
import com.github.tridays.unimqc.core.MessageExceptionWrapper;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AliyunMNSSender<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AliyunMNSSender.class);

    private final CloudQueue queue;

    private final Codec<T, byte[]> codec;

    @Setter
    private Consumer<MessageExceptionWrapper<Message>> sendErrorHandler;

    protected void doSend(byte[] b) {
        Message message = new Message(b);
        try {
            queue.putMessage(message);
            if (LOG.isTraceEnabled()) {
                LOG.trace("put message " + message.getMessageId());
            }
        } catch (Throwable e) {
            handleSendError(new MessageExceptionWrapper<>(message, e));
        }
    }

    public void send(T t) {
        doSend(codec.encode(t));
    }

    protected void handleSendError0(MessageExceptionWrapper<Message> ew) {
        if (LOG.isWarnEnabled()) {
            if (ew.getMessage() == null) {
                LOG.warn("send error: " + ew.getCause().getMessage(), ew.getCause());
            } else {
                LOG.warn("send error: messageId=" + ew.getMessage().getMessageId() + " " + ew.getCause().getMessage(), ew.getCause());
            }
        }
    }

    private void handleSendError(MessageExceptionWrapper<Message> ew) {
        if (sendErrorHandler == null) {
            handleSendError0(ew);
        } else {
            sendErrorHandler.accept(ew);
        }
    }

}
