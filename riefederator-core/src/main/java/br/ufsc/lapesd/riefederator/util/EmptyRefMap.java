package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class EmptyRefMap<K, V> extends AbstractMap<K, V> implements RefMap<K, V> {
    public static final @Nonnull RefMap<?, ?> INSTANCE = new EmptyRefMap<>();

    public static @Nonnull <K, V> RefMap<K, V> emptyMap() {
        //noinspection unchecked
        return (RefMap<K, V>)INSTANCE;
    }

    @Override public int size() {
        return 0;
    }
    @Override public boolean isEmpty() {
        return true;
    }
    @Override public boolean containsValue(Object value) {
        return false;
    }
    @Override public boolean containsKey(Object key) {
        return false;
    }
    @Override public V get(Object key) {
        return null;
    }
    @Override public boolean equals(Object o) {
        return (o instanceof Collection) && ((Collection<?>)o).isEmpty() ;
    }
    @Override public int hashCode() {
        return 0;
    }
    @Override public @Nonnull Set<Entry<K, V>> entrySet() {
        return Collections.emptySet();
    }
}
