package br.ufsc.lapesd.riefederator.deprecated;

import br.ufsc.lapesd.riefederator.util.RefMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.lang.System.identityHashCode;

public final class RefSortedPairMap<K, V> extends AbstractMap<K, V> implements RefMap<K, V> {
    private static final class Pair {
        private @Nonnull final Object k;
        private @Nonnull Object v;

        public Pair(@Nonnull Object k, @Nonnull Object v) {
            this.k = k;
            this.v = v;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Pair)) return false;
            Pair pair = (Pair) o;
            return k.equals(pair.k) &&
                    v.equals(pair.v);
        }

        @Override public int hashCode() {
            return identityHashCode(k) ^ v.hashCode();
        }
    }
    private static final @Nonnull Comparator<Pair> keyComparator = new Comparator<Pair>() {
        @Override public int compare(Pair o1, Pair o2) {
            return identityHashCode(o1.k) - identityHashCode(o2.k);
        }
    };

    private @Nonnull Pair[] data;
    private @Nullable HandleSet handleSet = null;
    private int hash, size;

    public RefSortedPairMap() {
        this(10);
    }

    public RefSortedPairMap(int capacity) {
        data = new Pair[capacity];
    }

    public RefSortedPairMap(@Nonnull Map<K, V> other) {
        size = other.size();
        data = new Pair[Math.max(size, 10)];
        int i = 0;
        for (Entry<K, V> e : other.entrySet())
            data[i++] = new Pair(e.getKey(), e.getValue());
        Arrays.sort(data, 0, size, keyComparator);
    }

    @Override public int size() {
        return size;
    }

    @Override public boolean containsValue(@Nullable Object value) {
        if (value == null) return false;
        for (int i = 0; i < size; i++) {
            if (data[i].v.equals(value)) return true;
        }
        return false;
    }

    @Override public boolean containsKey(Object key) {
        if (key == null) return false;
        Pair lookup = new Pair(key, this);
        int idx = Arrays.binarySearch(data, 0, size, lookup, keyComparator);
        if (idx >= 0) {
            int keyHash = identityHashCode(key);
            for (int i = idx; i < size; i++) {
                if (data[i].k == key)                            return true;
                else if (identityHashCode(data[i].k) != keyHash) return false;
            }
        }
        return false;
    }

    @Override public V get(Object key) {
        Pair pair = new Pair(key, this);
        int idx = Arrays.binarySearch(data, 0, size, pair, keyComparator);
        if (idx >= 0) {
            int hash = identityHashCode(key);
            for (int i = idx; i < size && identityHashCode(data[i].k) == hash; i++) {
                if (data[i].k == key) //noinspection unchecked
                    return (V) data[i].v;
            }
        }
        return null;
    }

    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        Pair lookup = new Pair(key, value);
        int idx = Arrays.binarySearch(data, 0, size, lookup, keyComparator);
        if (idx >= 0) {
            int keyHash = identityHashCode(key);
            for (int i = idx; i < size; i++) {
                Pair pair = data[i];
                if (pair.k == key) {
                    Object old = pair.v;
                    pair.v = value;
                    this.hash = 0;
                    //noinspection unchecked
                    return (V)old;
                } else if (identityHashCode(pair.k) != keyHash) {
                    idx = i;
                    break;
                }
            }
        } else {
            idx = (idx+1) * -1;
        }
        add(lookup, idx);
        return null;
    }

    private void add(@Nonnull Pair lookup, int idx) {
        if (size == data.length) {
            Pair[] next = new Pair[size*2];
            System.arraycopy(data, 0, next, 0, idx);
            next[idx] = lookup;
            System.arraycopy(data, idx, next, idx +1, size - idx);
            data = next;
        } else {
            System.arraycopy(data, idx, data, idx +1, size - idx);
            data[idx] = lookup;
        }
        this.hash = 0;
        ++size;
    }

    @Override public V remove(Object key) {
        Pair lookup = new Pair(key, this);
        int idx = Arrays.binarySearch(data, 0, size, lookup, keyComparator);
        if (idx >= 0) {
            int hash = identityHashCode(key);
            for (; idx < size; idx++) {
                Pair pair = data[idx];
                if (pair.k == key) break;
                else if (identityHashCode(pair.k) != hash) return null;
            }
            Object old = data[idx].v;
            remove(idx);
            //noinspection unchecked
            return (V)old;
        }
        return null;
    }

    private void remove(int idx) {
        System.arraycopy(data, idx + 1, data, idx, size - idx - 1);
        --size;
        this.hash = 0;
    }

    @Override public void clear() {
        size = 0;
    }

    @Override public @Nonnull Set<Entry<K, V>> entrySet() {
        if (handleSet == null)
            handleSet = new HandleSet();
        return handleSet;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (o instanceof Map) {
            @SuppressWarnings("unchecked") Map<K, V> m = (Map)o;
            if (m.size() != size) return false;
            if (o instanceof RefSortedPairMap) {
                @SuppressWarnings("unchecked") RefSortedPairMap<K, V> r = (RefSortedPairMap<K, V>) o;
                if (hash != 0 && r.hash != 0 && hash != r.hash)
                    return false;
                for (int i = 0; i < size; i++) {
                    Pair lp = data[i], rp = r.data[i];
                    if (lp.k != rp.k || lp.v != rp.v)
                        return false;
                }
                return true;
            } else {
                for (int i = 0; i < size; i++) {
                    //noinspection SuspiciousMethodCalls
                    if (!Objects.equals(m.get(data[i].k), data[i].v))
                        return false;
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        if (hash == 0) {
            int h = 0;
            for (int i = 0; i < size; i++)
                h += data[i].hashCode();
            hash = h;
        }
        return hash;
    }

    private class Handle implements Entry<K, V> {
        private final @Nonnull Pair pair;

        public Handle(int index) {
            this.pair = data[index];
        }

        @Override public K getKey() {
            //noinspection unchecked
            return (K)pair.k;
        }

        @Override public V getValue() {
            //noinspection unchecked
            return (V)pair.v;
        }

        @Override public V setValue(V value) {
            if (value == null) throw new NullPointerException();
            //noinspection unchecked
            V old = (V)pair.v;
            RefSortedPairMap.this.hash = 0;
            pair.v = value;
            return old;
        }

        @Override public String toString() {
            return getKey()+"="+getValue();
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Entry)) return false;
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            return pair.k.equals(e.getKey()) && pair.v.equals(e.getValue());
        }

        @Override public int hashCode() {
            return pair.hashCode();
        }
    }

    private class HandleIterator implements Iterator<Entry<K, V>> {
        private int nextIndex;

        @Override public boolean hasNext() {
            return nextIndex < size;
        }

        @Override public Entry<K, V> next() {
            if (!hasNext()) throw new NoSuchElementException();
            return new Handle(nextIndex++);
        }

        @Override public void remove() {
            if (nextIndex <= 0) throw new IllegalStateException("next() not called");
            RefSortedPairMap.this.remove(--nextIndex);
        }
    }

    private class HandleSet extends AbstractSet<Entry<K, V>> {
        @Override public @Nonnull Iterator<Entry<K, V>> iterator() {
            return new HandleIterator();
        }

        @Override public int size() {
            return size;
        }

        @Override public void clear() {
            RefSortedPairMap.this.clear();
        }

        @Override public boolean contains(Object o) {
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            V v = RefSortedPairMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        @Override public boolean add(Entry<K, V> e) {
            V old = RefSortedPairMap.this.put(e.getKey(), e.getValue());
            return old == null || old.equals(e.getValue());
        }

        @Override public boolean remove(Object o) {
            if (!(o instanceof Entry)) return false;
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            return RefSortedPairMap.this.remove(e.getKey(), e.getValue());
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof Collection)) return false;
            @SuppressWarnings("unchecked") Collection<Entry<K, V>> c = (Collection<Entry<K, V>>) o;
            if (c.size() != size) return false;
            for (Entry<K, V> entry : c) {
                if (!Objects.equals(get(entry.getKey()), entry.getValue()))
                    return false;
            }
            return true;
        }

        public int hashCode() {
            return RefSortedPairMap.this.hashCode();
        }
    }
}
