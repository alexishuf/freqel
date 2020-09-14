package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ListRefHashMultimap<K, V> extends RefHashMap<K, List<V>> {
    public ListRefHashMultimap() {
    }

    public ListRefHashMultimap(int buckets, int bucketSize) {
        super(buckets, bucketSize);
    }

    public ListRefHashMultimap(int capacity) {
        super(capacity);
    }

    public boolean putValue(@Nonnull K key, @Nonnull V value) {
        List<V> list = get(key);
        if (list == null) {
            list = new ArrayList<>();
            put(key, list);
        }
        list.add(value);
        return true;
    }

    public boolean removeValue(@Nonnull K key, @Nonnull V value) {
        List<V> list = get(key);
        if (list == null) return false;
        if (!list.remove(value)) return false;
        if (list.isEmpty()) remove(key);
        return true;
    }
}
