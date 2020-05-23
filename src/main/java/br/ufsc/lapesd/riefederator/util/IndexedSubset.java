package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Spliterator.*;

public class IndexedSubset<T> extends AbstractSet<T> implements Set<T> {
    protected final @Nonnull IndexedSet<T> parent;
    protected final @Nonnull BitSet bitSet;

    protected static final @Nonnull BitSet emptyBitSet = new BitSet(0);

    public IndexedSubset(@Nonnull IndexedSet<T> parent, @Nonnull BitSet bitSet) {
        this.parent = parent;
        this.bitSet = bitSet;
    }

    public static @Nonnull <U> IndexedSubset<U> empty() {
        assert emptyBitSet.cardinality() == 0;
        assert emptyBitSet.size() == 0;
        return new IndexedSubset<>(IndexedSet.empty(), emptyBitSet);
    }

    public @Nonnull IndexedSet<T> getParent() {
        return parent;
    }

    public @Nonnull BitSet getBitSet() {
        return bitSet;
    }

    @SuppressWarnings("ReferenceEquality")
    public void intersect(@Nonnull Collection<? extends T> o) {
        if (o instanceof IndexedSubset && ((IndexedSubset<?>) o).getParent() == parent) {
            bitSet.and(((IndexedSubset<?>) o).getBitSet());
        } else {
            bitSet.and(parent.subset(o).getBitSet());
        }
    }

    @SuppressWarnings("ReferenceEquality")
    public int union(@Nonnull Collection<? extends T> o) {
        int old = size();
        if (o instanceof IndexedSubset && ((IndexedSubset<?>) o).getParent() == parent) {
            this.bitSet.or(((IndexedSubset<?>) o).getBitSet());
        } else {
            this.bitSet.or(parent.subset(o).getBitSet());
        }
        assert size() >= old;
        return size() - old;
    }

    @SuppressWarnings("ReferenceEquality")
    public int difference(@Nonnull Collection<?> o) {
        if (o instanceof IndexedSubset && ((IndexedSubset<?>) o).getParent() == parent) {
            int old = size();
            this.bitSet.andNot(((IndexedSubset<?>) o).getBitSet());
            assert size() <= old;
            return old - size();
        } else {
            int count = 0;
            for (Object element : o)
                count += remove(element) ? 1 : 0;
            return count;
        }
    }

    public @Nonnull IndexedSubset<T> copy() {
        BitSet bitSet = new BitSet(parent.size());
        bitSet.or(this.bitSet);
        return new IndexedSubset<>(parent, bitSet);
    }

    public @Nonnull IndexedSubset<T> createIntersection(@Nonnull Collection<? extends T> coll) {
        IndexedSubset<T> copy = copy();
        copy.intersect(coll);
        return copy;
    }

    public @Nonnull IndexedSubset<T> createUnion(@Nonnull Collection<? extends T> collection) {
        IndexedSubset<T> copy = copy();
        copy.union(collection);
        return copy;
    }

    public @Nonnull IndexedSubset<T> createDifference(@Nonnull Collection<? extends T> coll) {
        IndexedSubset<T> copy = copy();
        copy.difference(coll);
        return copy;
    }

    /* --- implement object methods --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == parent)
            return size() == parent.size();
        if (o instanceof IndexedSubset) {
            IndexedSubset<?> that = (IndexedSubset<?>) o;
            return size() == that.size() && containsAll(that);
        } else if (o instanceof Set) {
            Set<?> that = (Set<?>) o;
            return that.size() == size() && ((Set<?>) o).containsAll(this);
        } else {
            Collection<?> that = (Collection<?>) o;
            return that.size() == size() && containsAll(that);
        }
    }

    @Override
    public int hashCode() {
        // do not cache, since getBitSet() can be modified
        int size = size(), out = 0;
        int[] codes = new int[size];
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i+1))
            codes[out++] = parent.get(i).hashCode();
        Arrays.sort(codes);
        return Arrays.hashCode(codes);
    }

    /* --- implement List & Set methods --- */

    @Override
    public boolean contains(Object o) {
        //noinspection SuspiciousMethodCalls
        int idx = parent.indexOf(o);
        return idx >= 0 && bitSet.get(idx);
    }

    public boolean containsAny(@Nonnull Collection<T> other) {
        return other.stream().anyMatch(this::contains);
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public boolean containsAll(@Nonnull Collection<?> c) {
        if (c instanceof IndexedSubset) {
            IndexedSubset<?> that = (IndexedSubset<?>) c;
            if (that.getParent() == parent) {
                BitSet copy = new BitSet(parent.size());
                copy.or(bitSet);
                copy.or(that.getBitSet());
                return copy.equals(bitSet);
            }
        } else if (c == parent) {
            return size() == parent.size();
        }
        if (size() < c.size()) return false;
        for (Object o : c) {
            if (!contains(o)) return false;
        }
        return true;
    }

    @Override
    public @Nonnull Iterator<T> iterator() {
        return new Iterator<T>() {
            int idx = 0;

            @Override
            public boolean hasNext() {
                idx = bitSet.nextSetBit(idx);
                return idx >= 0;
            }

            @Override
            public T next() {
                if (!hasNext()) throw new NoSuchElementException("Iterator past the end");
                int old = this.idx;
                ++this.idx;
                return parent.get(old);
            }
        };
    }

    @Override
    public boolean add(T t) {
        int idx = parent.indexOf(t);
        checkArgument(idx >= 0, "Cannot add "+t+" since it is not in getParent()");
        boolean old = bitSet.get(idx);
        if (!old)
            bitSet.set(idx);
        return !old;
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends T> c) {
        return union(c) > 0;
    }

    @Override
    public boolean remove(Object o) {
        //noinspection SuspiciousMethodCalls
        int idx = parent.indexOf(o);
        if (idx < 0) return false;
        boolean old = bitSet.get(idx);
        if (old)
            bitSet.set(idx, false);
        return old;
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        return difference(c) > 0;
    }

    @Override
    public void clear() {
        bitSet.clear();
    }

    @Override
    public int size() {
        return bitSet.cardinality();
    }

    @Override
    public Spliterator<T> spliterator() {
        return Spliterators.spliterator(iterator(), size(),
                DISTINCT|NONNULL|SIZED);
    }
}
