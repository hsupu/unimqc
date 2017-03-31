package com.github.tridays.unimqc;

/**
 * @author xp
 */
public interface Codec<A, B> {

    B encode(A a);

    A decode(B b);

}
