package br.ufsc.lapesd.freqel.util.ref;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;

public class IdentityHashSet<T> extends AbstractSet<T> implements RefSet<T> {
    private final IdentityHashMap<T, Object> map;
    private int hash = 0;
    private static final Object PRESENT = new Object();

    public IdentityHashSet(@Nonnull IdentityHashMap<T, ?> map) {
        //noinspection unchecked
        this.map = (IdentityHashMap<T, Object>) map;
    }

    public IdentityHashSet(@Nonnull Collection<? extends T> collection) {
        map = new IdentityHashMap<>((int) ((float) collection.size() / 0.75f + 1.0f));
        for (T value : collection)
            map.put(value, PRESENT);
    }

    public IdentityHashSet(int capacity) {
        map = new IdentityHashMap<>(capacity);
    }

    public IdentityHashSet() {
        map = new IdentityHashMap<>();
    }

    @SafeVarargs public static @Nonnull <U> IdentityHashSet<U> of (U... values) {
        IdentityHashMap<U, Object> map = new IdentityHashMap<>(values.length);
        for (U value : values)
            map.put(value, PRESENT);
        return new IdentityHashSet<>(map);
    }

    private class It implements Iterator<T> {
        private final Iterator<T> mapIt;

        public It(Iterator<T> mapIt) {
            this.mapIt = mapIt;
        }

        @Override public boolean hasNext() {
            return mapIt.hasNext();
        }

        @Override public T next() {
            return mapIt.next();
        }

        @Override public void remove() {
            mapIt.remove();
            hash = 0;
        }
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return new It(map.keySet().iterator());
    }

    @Override public int size() {
        return map.size();
    }

    @Override public boolean contains(Object o) {
        //noinspection SuspiciousMethodCalls
        return map.containsKey(o);
    }

    @Override public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            //noinspection SuspiciousMethodCalls
            if (!map.containsKey(o)) return false;
        }
        return true;
    }

    @Override public boolean add(T t) {
        return map.put(t, PRESENT) == null;
    }

    @Override public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override public void clear() {
        map.clear();
    }

    @Override public int hashCode() {
        if (hash == 0) {
            int h = 0;
            for (T k : map.keySet())
                h += System.identityHashCode(k);
            hash = h;
        }
        return hash;
    }

    @Override public boolean equals(@Nullable Object o) {
        if (o == this) return true;
        if (!(o instanceof Collection)) return false;
        Collection<?> coll = (Collection<?>) o;
        if (coll.size() != size()) return false;
        for (Object k : coll) {
            //noinspection SuspiciousMethodCalls
            if (!map.containsKey(k)) return false;
        }
        return true;
    }
}
