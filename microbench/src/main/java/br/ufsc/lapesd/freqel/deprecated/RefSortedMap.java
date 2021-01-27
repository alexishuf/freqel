package br.ufsc.lapesd.freqel.deprecated;

import br.ufsc.lapesd.freqel.util.RefMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.lang.System.identityHashCode;

public final class RefSortedMap<K, V> extends AbstractMap<K, V> implements RefMap<K, V> {
    private @Nonnull Object[] keys, values;
    private @Nullable HandleSet handleSet = null;
    private int hash, size;
    private static final @Nonnull Comparator<Object> keyComparator = Comparator.comparingInt(System::identityHashCode);

    public RefSortedMap() {
        this(10);
    }

    public RefSortedMap(int capacity) {
        keys = new Object[capacity];
        values = new Object[capacity];
    }

    public RefSortedMap(@Nonnull Map<K, V> other) {
        size = other.size();
        int capacity = Math.max(size, 10);
        keys = new Object[capacity];
        values = new Object[capacity];
        Integer[] indices = new Integer[capacity];
        Object[] scratch = new Object[capacity], tmp;
        int size = 0;
        for (Entry<K, V> e : other.entrySet()) {
            indices[size]  = size;
            keys[size]     = e.getKey();
            values[size++] = e.getValue();
        }
        for (int i = size; i < indices.length; i++)
            indices[i] = Integer.MAX_VALUE;
        Arrays.sort(indices, 0, this.size,
                    Comparator.comparing(i -> identityHashCode(keys[i])));
        for (int i = 0; i < size; i++)
            scratch[i] = keys[indices[i]];
        tmp = scratch;
        scratch = keys;
        keys = tmp;
        for (int i = 0; i < size; i++)
            scratch[i] = values[indices[i]];
        values = scratch;
    }

    @Override public int size() {
        return size;
    }

    @Override public boolean containsValue(@Nullable Object value) {
        if (value == null) return false;
        for (int i = 0; i < size; i++) {
            if (values[i].equals(value)) return true;
        }
        return false;
    }

    @Override public boolean containsKey(Object key) {
        if (key == null) return false;
        int idx = Arrays.binarySearch(keys, 0, size, key, keyComparator);
        if (idx >= 0) {
            int keyHash = identityHashCode(key);
            for (int i = idx; i < size; i++) {
                if (keys[i] == key)                            return true;
                else if (identityHashCode(keys[i]) != keyHash) return false;
            }
        }
        return false;
    }

    @Override public V get(Object key) {
        int idx = Arrays.binarySearch(keys, 0, size, key, keyComparator);
        if (idx >= 0) {
            int hash = identityHashCode(key);
            for (int i = idx; i < size && identityHashCode(keys[i]) == hash; i++) {
                if (keys[i] == key) //noinspection unchecked
                    return (V) values[i];
            }
        }
        return null;
    }

    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        int idx = Arrays.binarySearch(keys, 0, size, key, keyComparator);
        if (idx >= 0) {
            int keyHash = identityHashCode(key);
            for (int i = idx; i < size; i++) {
                if (keys[i] == key) {
                    Object old = values[i];
                    values[i] = value;
                    this.hash = 0;
                    //noinspection unchecked
                    return (V)old;
                } else if (identityHashCode(keys[i]) != keyHash) {
                    idx = i;
                    break;
                }
            }
        } else {
            idx = (idx+1) * -1;
        }
        add(key, value, idx);
        return null;
    }

    private void add(@Nonnull Object key, @Nonnull Object value, int idx) {
        if (size == keys.length) {
            Object[] nextKeys = new Object[size*2];
            Object[] nextValues = new Object[size*2];
            System.arraycopy(keys,   0, nextKeys,   0, idx);
            System.arraycopy(values, 0, nextValues, 0, idx);
            nextKeys[idx] = key;
            nextValues[idx] = value;
            System.arraycopy(keys,   idx, nextKeys,   idx +1, size - idx);
            System.arraycopy(values, idx, nextValues, idx +1, size - idx);
            keys = nextKeys;
            values = nextValues;
        } else {
            System.arraycopy(keys,   idx, keys,   idx +1, size - idx);
            System.arraycopy(values, idx, values, idx +1, size - idx);
            keys[idx] = key;
            values[idx] = value;
        }
        this.hash = 0;
        ++size;
    }

    @Override public V remove(Object key) {
        int idx = Arrays.binarySearch(keys, 0, size, key, keyComparator);
        if (idx >= 0) {
            int hash = identityHashCode(key);
            for (; idx < size; idx++) {
                Object candidate = keys[idx];
                if      (candidate == key)                    break;
                else if (identityHashCode(candidate) != hash) return null;
            }
            Object old = values[idx];
            remove(idx);
            //noinspection unchecked
            return (V)old;
        }
        return null;
    }

    private void remove(int idx) {
        System.arraycopy(keys,   idx + 1, keys,   idx, size - idx - 1);
        System.arraycopy(values, idx + 1, values, idx, size - idx - 1);
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
            @SuppressWarnings("unchecked") Map<K, V> m = (Map<K, V>)o;
            if (m.size() != size) return false;
            if (o instanceof RefSortedMap) {
                @SuppressWarnings("unchecked") RefSortedMap<K, V> r = (RefSortedMap<K, V>) o;
                if (hash != 0 && r.hash != 0 && hash != r.hash)
                    return false;
                for (int i = 0; i < size; i++) {
                    Object lk =   keys[i], rk =   r.keys[i];
                    Object lv = values[i], rv = r.values[i];
                    if (lk != rk || lv != rv)
                        return false;
                }
                return true;
            } else {
                for (int i = 0; i < size; i++) {
                    //noinspection SuspiciousMethodCalls
                    if (!Objects.equals(m.get(keys[i]), values[i]))
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
                h += identityHashCode(keys[i]) ^ values[i].hashCode();
            hash = h;
        }
        return hash;
    }

    private class Handle implements Entry<K, V> {
        private int index;
        private K k;
        private V v;

        @SuppressWarnings("unchecked") public Handle(int index) {
            this.index = index;
            this.k = (K)keys[index];
            this.v = (V)values[index];
        }

        @Override public K getKey() { return k; }
        @Override public V getValue() { return v; }

        @Override public V setValue(V value) {
            if (value == null)
                throw new NullPointerException();
            if (keys[index] != k)
                throw new ConcurrentModificationException();
            if (values[index] != v)
                throw new ConcurrentModificationException();
            V old = v;
            RefSortedMap.this.hash = 0;
            values[index] = value;
            return old;
        }

        @Override public String toString() {
            return getKey()+"="+getValue();
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Entry)) return false;
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            return k.equals(e.getKey()) && v.equals(e.getValue());
        }

        @Override public int hashCode() {
            return identityHashCode(k) ^ v.hashCode();
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
            RefSortedMap.this.remove(--nextIndex);
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
            RefSortedMap.this.clear();
        }

        @Override public boolean contains(Object o) {
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            V v = RefSortedMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        @Override public boolean add(Entry<K, V> e) {
            V old = RefSortedMap.this.put(e.getKey(), e.getValue());
            return old == null || old.equals(e.getValue());
        }

        @Override public boolean remove(Object o) {
            if (!(o instanceof Entry)) return false;
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            return RefSortedMap.this.remove(e.getKey(), e.getValue());
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
            return RefSortedMap.this.hashCode();
        }
    }
}
