package br.ufsc.lapesd.riefederator.util.ref;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

@SuppressWarnings("ClassExtendsConcreteCollection")
public class ListIdentityMultimap<K, V> extends IdentityHashMap<K, List<V>> {
    public ListIdentityMultimap() {
    }

    public ListIdentityMultimap(int capacity) {
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
