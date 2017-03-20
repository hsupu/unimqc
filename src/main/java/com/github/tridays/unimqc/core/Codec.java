package com.github.tridays.unimqc.core;

/**
 * @author xp
 */
public interface Codec<A, B> {

    B encode(A a);

    A decode(B b);

}
