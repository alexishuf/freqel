package br.ufsc.lapesd.riefederator.deprecated;

import br.ufsc.lapesd.riefederator.util.RefMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.lang.Integer.bitCount;
import static java.lang.Integer.highestOneBit;
import static java.lang.System.identityHashCode;

public class RefHashMapFastGrowth<K, V> extends AbstractMap<K, V> implements RefMap<K, V> {

    @SuppressWarnings("NotNullFieldNotInitialized") private @Nonnull Object[] keys;
    @SuppressWarnings("NotNullFieldNotInitialized") private @Nonnull Object[] values;
    private @Nullable EntrySet entrySet = null;
    private int buckets, bucketSize, bucketMask, hash = 0, size = 0;

    public RefHashMapFastGrowth() {
        this(16, 3);
    }

    private void alloc(int buckets, int bucketSize, @Nullable Object[] keys,
                       @Nullable Object[] values) {
        assert buckets > 0 && bucketSize > 0;
        int tmp = highestOneBit(buckets);
        this.buckets = buckets = tmp < buckets ? tmp << 1 : tmp;
        assert bitCount(buckets) == 1;
        int capacity = buckets * bucketSize;
        this.bucketMask = buckets - 1;
        this.bucketSize = bucketSize;
        this.keys = new Object[capacity];
        this.values = new Object[capacity];

        if (keys != null) {
            assert values != null;
            assert values.length == keys.length;
            assert capacity > keys.length;
            for (int i = 0; i < keys.length; i++) {
                Object key = keys[i];
                if (key != null) {
                    int begin = (identityHashCode(key) & bucketMask) * this.bucketSize;
                    boolean ok = false;
                    for (int j = begin, end = begin+bucketSize; !ok && j < end; j++) {
                        if ((ok = this.keys[j] == null)) {
                            this.keys[j] = key;
                            this.values[j] = values[i];
                        }
                    }
                    assert ok;
                }
            }
        }
    }

    public RefHashMapFastGrowth(int buckets, int bucketSize) {
        alloc(buckets, bucketSize, null, null);
    }

    public RefHashMapFastGrowth(int capacity) {
        alloc(capacity/2 + 1,  3, null, null);
    }

    public RefHashMapFastGrowth(@Nonnull Map<K, V> other) {
        this(other.size()/2+1, 3);
        assert buckets*bucketSize >= other.size();
        assert keys.length == buckets*bucketSize;
        assert keys.length == values.length;
        for (Entry<K, V> e : other.entrySet())
            put(e.getKey(), e.getValue());
    }

    @Override public int size() {
        return size;
    }

    @Override public boolean containsValue(Object value) {
        if (value == null) return false;
        for (Object v : values) {
            if (v != null && v.equals(value)) return true;
        }
        return false;
    }

    @Override public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override public V get(Object key) {
        if (key == null) return null;
        int begin = (identityHashCode(key) & bucketMask) * bucketSize;
        for (int i = begin, end = begin+bucketSize; i < end; i++) {
            if      (keys[i] == null) return null;
            else if (keys[i] == key) //noinspection unchecked
                return (V)values[i];
        }
        return null;
    }

    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        int begin = (identityHashCode(key) & bucketMask) * bucketSize;
        for (int i = begin, end = begin + bucketSize; i < end; i++) {
            if (keys[i] == null) {
                keys[i] = key;
                values[i] = value;
                ++size;
                hash = 0;
                return null;
            } else if (keys[i] == key) {
                @SuppressWarnings("unchecked") V old = (V)values[i];
                values[i] = value;
                hash = 0;
                return old;
            }
        }
        alloc(buckets*2, bucketSize+1, keys, values);
        return put(key, value); //recursive call will not recurse
    }

    private @Nullable V remove(int i, int bucketEnd) {
        @SuppressWarnings("unchecked") V old = (V)values[i];
        int bucketLast = bucketEnd-1;
        System.arraycopy(keys, i+1, keys, i, bucketLast-i);
        System.arraycopy(values, i+1, values, i, bucketLast-i);
        keys[bucketLast] = values[bucketLast] = null;
        --size;
        hash = 0;
        return old;
    }

    @Override public V remove(Object key) {
        if (key == null) return null;
        int begin = (identityHashCode(key) & bucketMask) * bucketSize;
        for (int i = begin, end = begin + bucketSize; i < end; i++) {
            if (keys[i] == null)
                return null;
            else if (keys[i] == key)
                return remove(i, end);
        }
        return null;
    }

    @Override public void clear() {
        size = 0;
        hash = 0;
        Arrays.fill(keys, null);
        Arrays.fill(values, null);
    }

    @Override public int hashCode() {
        if (hash == 0) {
            int h = 0;
            for (int i = 0; i < keys.length; i += bucketSize) {
                for (int j = i, end = i+bucketSize; j < end; j++) {
                    if (keys[j] == null) break;
                    else h += identityHashCode(keys[j]) ^ values[j].hashCode();
                }
            }
            hash = h;
        }
        return hash;
    }

    @Override public @Nonnull Set<Entry<K, V>> entrySet() {
        return entrySet == null ? (entrySet = new EntrySet()) : entrySet;
    }

    private class Handle implements Entry<K, V> {
        private int index;
        private @Nonnull K k;
        private @Nonnull V v;

        @SuppressWarnings("unchecked")
        public Handle(int index) {
            this.index = index;
            k = (K)keys[index];
            v = (V)values[index];
        }

        @Override public K getKey() {
            return k;
        }

        @Override public V getValue() {
            return v;
        }

        @Override public V setValue(V value) {
            if (value == null)
                throw new NullPointerException();
            V old = v;
            if (keys[index] != k)
                throw new ConcurrentModificationException();
            if (values[index] != v)
                throw new ConcurrentModificationException();
            values[index] = v = value;
            return old;
        }

        @Override public @Nonnull String toString() {
            return k+"="+v;
        }

        @Override public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof Entry)) return false;
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            return k == e.getKey() && v.equals(e.getValue());
        }

        @Override public int hashCode() {
            return identityHashCode(k) ^ v.hashCode();
        }
    }

    private class EntryIterator implements Iterator<Entry<K, V>> {
        private int lastIndex = -1, index = -1;

        public EntryIterator() {
            advance();
        }

        private void advance() {
            lastIndex = index++;
            while (index < keys.length && keys[index] == null)
                index = (index/bucketSize + 1) * bucketSize; //jump to next bucket
        }

        @Override public boolean hasNext() {
            return index < keys.length;
        }

        @Override public Entry<K, V> next() {
            Handle handle = new Handle(index);
            advance();
            return handle;
        }

        @Override public void remove() {
            if (lastIndex < 0) throw new IllegalStateException("next() not called");
            RefHashMapFastGrowth.this.remove(lastIndex, (lastIndex/bucketSize + 1) * bucketSize);
            index = lastIndex-1;
            advance();
        }
    }

    private class EntrySet extends AbstractSet<Entry<K, V>> {
        @Override public @Nonnull Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override public int size() {
            return size;
        }

        @Override public boolean contains(Object o) {
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            V v = RefHashMapFastGrowth.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        @Override public boolean add(Entry<K, V> e) {
            V old = RefHashMapFastGrowth.this.put(e.getKey(), e.getValue());
            return !Objects.equals(old, e.getValue());
        }

        @Override public boolean remove(Object o) {
            @SuppressWarnings("unchecked") Entry<K, V> e = (Entry<K, V>) o;
            return RefHashMapFastGrowth.this.remove(e.getKey(), e.getValue());
        }

        @Override public void clear() {
            RefHashMapFastGrowth.this.clear();
        }

        @Override public int hashCode() {
            return RefHashMapFastGrowth.this.hashCode();
        }
    }
}
