package com.github.tridays.unimqc.sqs;

import java.util.*;
import java.util.function.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.github.tridays.unimqc.Codec;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * @author xp
 */
@RequiredArgsConstructor
public class AmazonSQSSender<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AmazonSQSSender.class);

    private final AmazonSQS amazonSQS;

    private final String queueUrl;

    private final Codec<T, byte[]> codec;

    @Setter
    private Consumer<Throwable> sendErrorHandler;

    protected void doSend(byte[] b) {
        SendMessageRequest request = new SendMessageRequest(queueUrl, Base64.getEncoder().encodeToString(b));
        try {
            SendMessageResult result = amazonSQS.sendMessage(request);
            if (LOG.isTraceEnabled()) {
                LOG.trace("send message " + result.getMessageId());
            }
        } catch (Throwable e) {
            handleSendError(e);
        }
    }

    public void send(T t) {
        doSend(codec.encode(t));
    }

    protected void handleSendError0(Throwable e) {
        if (LOG.isWarnEnabled()) {
            LOG.warn("send error: " + e.getMessage(), e);
        }
    }

    private void handleSendError(Throwable e) {
        if (sendErrorHandler == null) {
            handleSendError0(e);
        } else {
            sendErrorHandler.accept(e);
        }
    }

}
