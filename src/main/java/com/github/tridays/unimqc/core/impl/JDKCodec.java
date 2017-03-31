package com.github.tridays.unimqc.core.impl;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tridays.unimqc.core.Codec;

/**
 * @author xp
 */
public class JDKCodec<T> implements Codec<T, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(JDKCodec.class);

    public static JDKCodec INSTANCE = new JDKCodec();

    @Override
    public byte[] encode(T o) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(o);
                oos.flush();
            }
            return bos.toByteArray();
        } catch (Throwable e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(e.getMessage(), e);
            }
            return null;
        }
    }

    @Override
    public T decode(byte[] b) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(b)) {
            try (ObjectInputStream ois = new ObjectInputStream(bis)) {
                @SuppressWarnings("unchecked")
                T t = (T) ois.readObject();
                return t;
            }
        } catch (Throwable e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(e.getMessage(), e);
            }
            return null;
        }
    }
}
