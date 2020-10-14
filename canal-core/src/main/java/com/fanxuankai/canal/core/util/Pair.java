package com.fanxuankai.canal.core.util;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author fanxuankai
 */
public class Pair<K, V> implements Serializable {
    private final K k;
    private final V v;

    public Pair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public static <K, V> Pair<K, V> of(K k, V v) {
        return new Pair<>(k, v);
    }

    public K getKey() {
        return k;
    }

    public V getValue() {
        return v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(k, pair.k) &&
                Objects.equals(v, pair.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k, v);
    }

}
