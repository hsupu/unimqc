package com.github.tridays.unimqc;

/**
 * @author xp
 */
public interface MQClient<T> {

    void init() throws Exception;

    void send(T t);

}
