package br.ufsc.lapesd.riefederator.util;

import br.ufsc.lapesd.riefederator.util.ref.RefSet;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

public class RefHashSet<T> extends AbstractSet<T> implements RefSet<T> {
    private final @Nonnull RefHashMap<T, Object> map;
    static final @Nonnull Present PRESENT = new Present();

    static final class Present {
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override public boolean equals(Object ignored) { return true; }
        @Override public int hashCode() { return 0; }
    }

    RefHashSet(@Nonnull RefHashMap<T, ?> map) {
        //noinspection unchecked
        this.map = (RefHashMap<T, Object>) map;
    }

    public RefHashSet() {
        map = new RefHashMap<>();
    }

    public RefHashSet(int capacity) {
        map = new RefHashMap<>(capacity);
    }

    public RefHashSet(Collection<T> collection) {
        map = new RefHashMap<>(collection.size());
        for (T value : collection)
            map.put(value, PRESENT);
    }

    public static @Nonnull <T> RefHashSet<T> of(T... values) {
        RefHashSet<T> set = new RefHashSet<>(values.length);
        for (T value : values)
            set.add(value);
        return set;
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return map.keySet().iterator();
    }

    @Override public int size() {
        return map.size();
    }

    @Override public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override public boolean contains(Object o) {
        //noinspection SuspiciousMethodCalls
        return map.containsKey(o);
    }

    @Override public boolean add(T t) {
        return map.put(t, PRESENT) == null;
    }

    @Override public boolean remove(Object key) {
        return map.remove(key) != null;
    }

    @Override public void clear() {
        map.clear();
    }

    @Override public int hashCode() {
        return map.keySet().hashCode();
    }
}
