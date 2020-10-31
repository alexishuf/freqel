package br.ufsc.lapesd.riefederator.util.indexed.subset;

import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.NotInParentException;
import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Predicate;

import static br.ufsc.lapesd.riefederator.util.indexed.subset.BitsetOps.subtract;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class SimpleIndexSubset<T> extends AbstractSet<T> implements IndexSubset<T> {
    protected final @Nonnull IndexSet<T> parent;
    protected final @Nonnull Bitset bs;


    public SimpleIndexSubset(@Nonnull IndexSet<T> parent, @Nonnull Bitset bitSet) {
        assert !(parent instanceof IndexSubset)
                : "Creating a IndexedSubset of an IndexedSubst works but is inefficient!";
        this.parent = parent;
        this.bs = bitSet;
    }

    /* --- --- Implement IndexedSubset methods --- --- */

    @Override public @Nonnull IndexSet<T> getParent() { return parent; }
    @Override public @Nonnull Bitset getBitset() { return bs; }

    @Override public @Nonnull ImmIndexSubset<T> asImmutable() {
        return new SimpleImmIndexSubset<>(parent, bs);
    }

    @Override public @Nonnull IndexSubset<T> complement() {
        BitsetOps.complement(parent, bs);
        return this;
    }

    @Override public @Nonnull IndexSubset<T> intersect(@Nonnull Collection<? extends T> coll) {
        BitsetOps.intersect(parent, bs, coll);
        return this;
    }

    @Override public @Nonnull IndexSubset<T> union(@Nonnull Collection<? extends T> coll) {
        BitsetOps.union(parent, bs, coll);
        return this;
    }

    @Override public @Nonnull IndexSubset<T> minus(@Nonnull Collection<? extends T> coll) {
        BitsetOps.subtract(parent, bs, coll);
        return this;
    }

    @Override public @Nonnull IndexSubset<T> symDiff(@Nonnull Collection<? extends T> coll) {
        BitsetOps.symDiff(parent, bs, coll);
        return this;
    }

    @Override public @Nonnull IndexSubset<T> copy() {
        return new SimpleIndexSubset<>(parent, bs.copy());
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableCopy() {
        return new SimpleImmIndexSubset<>(parent, bs.copy());
    }

    @Override public @Nonnull IndexSubset<T> createComplement() {
        IndexSubset<T> c = copy();c.complement();return c;
    }

    @Override public @Nonnull ImmIndexSubset<T> createImmutableComplement() {
        return new SimpleImmIndexSubset<>(parent, BitsetOps.complement(parent, bs.copy()));
    }

    @Override
    public @Nonnull IndexSubset<T> createIntersection(@Nonnull Collection<? extends T> coll) {
        return new SimpleIndexSubset<>(parent, BitsetOps.intersect(parent, bs.copy(), coll));
    }

    @Override public @Nonnull ImmIndexSubset<T>
    createImmutableIntersection(@Nonnull Collection<? extends T> coll) {
        return new SimpleImmIndexSubset<>(parent, BitsetOps.intersect(parent, bs.copy(), coll));
    }

    @Override
    public @Nonnull IndexSubset<T> createUnion(@Nonnull Collection<? extends T> collection) {
        return new SimpleIndexSubset<>(parent, BitsetOps.union(parent, bs.copy(), collection));
    }

    @Override
    public @Nonnull ImmIndexSubset<T>
    createImmutableUnion(@Nonnull Collection<? extends T> collection) {
        Bitset bs = BitsetOps.union(parent, this.bs.copy(), collection);
        return new SimpleImmIndexSubset<>(parent, bs);
    }

    @Override public @Nonnull <U extends T> IndexSubset<T> createUnion(@Nonnull U value) {
        IndexSubset<T> c = copy(); c.add(value); return c;
    }

    @Override
    public @Nonnull <U extends T> ImmIndexSubset<T> createImmutableUnion(@Nonnull U value) {
        return new SimpleImmIndexSubset<>(parent, BitsetOps.union(parent, bs.copy(), value));
    }

    @Override
    public @Nonnull IndexSubset<T> createSymDiff(@Nonnull Collection<? extends T> coll) {
        return new SimpleIndexSubset<>(parent, BitsetOps.symDiff(parent, bs.copy(), coll));
    }

    @Override
    public @Nonnull ImmIndexSubset<T> createImmutableSymDiff(@Nonnull Collection<? extends T> c) {
        return new SimpleImmIndexSubset<>(parent, BitsetOps.symDiff(parent, bs.copy(), c));
    }

    @Override
    public @Nonnull IndexSubset<T> createMinus(@Nonnull Collection<? extends T> coll) {
        IndexSubset<T> c = copy(); c.removeAll(coll); return c;
    }

    @Override public boolean hasIndex(int idx, @Nonnull IndexSet<T> expectedParent) {
        checkElementIndex(idx, parent.size());
        assert parent == expectedParent;
        return bs.get(idx);
    }

    @Override public @Nonnull T getAtIndex(int idx, @Nonnull IndexSet<T> expectedParent) {
        checkElementIndex(idx, parent.size());
        assert parent == expectedParent;
        return parent.get(idx);
    }

    @Override public boolean setIndex(int index, @Nonnull IndexSet<T> expectedParent) {
        checkElementIndex(index, parent.size());
        assert parent == expectedParent;
        boolean old = bs.get(index);
        if (!old)
            bs.set(index);
        return !old;
    }

    /* --- --- Implement IndexedSet methods --- --- */

    protected class EntryIt implements Iterator<Map.Entry<T, Integer>> {
        int lastBit = -1, nextBit = -2;

        @Override public boolean hasNext() {
            if (nextBit == -2)
                nextBit = bs.nextSetBit(lastBit+1);
            return nextBit >= 0;
        }

        @Override public Map.Entry<T, Integer> next() {
            if (!hasNext()) throw new NoSuchElementException();
            lastBit = nextBit;
            nextBit = bs.nextSetBit(nextBit+1);
            T value = parent.get(lastBit);
            return new UnmodifiableMapEntry<>(value, lastBit);
        }
    }

    @Override public @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator() {
        return new EntryIt();
    }

    @Override public @Nonnull IndexSubset<T> fullSubset() {
        return new SimpleIndexSubset<>(parent, bs.copy());
    }

    @Override public @Nonnull IndexSubset<T> emptySubset() {
        return new SimpleIndexSubset<>(parent, Bitsets.create(parent.size()));
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull Collection<? extends T> coll) {
        return new SimpleIndexSubset<>(parent, BitsetOps.subset(parent, this, this.bs, coll));
    }

    @Override
    public @Nonnull IndexSubset<T> subsetExpanding(@Nonnull Collection<? extends T> coll) {
        return new SimpleIndexSubset<>(parent, BitsetOps.subsetExpanding(parent, coll));
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull Bitset subset) {
        if (subset.length() > size())
            throw new NotInParentException(subset, this);
        return new SimpleIndexSubset<>(parent, subset);
    }

    @Override public @Nonnull ImmIndexSubset<T>
    immutableSubset(@Nonnull Collection<? extends T> coll) {
        return new SimpleImmIndexSubset<>(parent, BitsetOps.subset(parent, this, this.bs, coll));
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubsetExpanding(@Nonnull Collection<? extends T> c) {
        return new SimpleImmIndexSubset<>(parent, BitsetOps.subsetExpanding(parent, c));
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Bitset subset) {
        if (subset.length() > size())
            throw new NotInParentException(subset, this);
        return new SimpleImmIndexSubset<>(parent, subset);
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate) {
        return new SimpleIndexSubset<>(parent, subtract(parent, bs.copy(), predicate.negate()));
    }

    @Override public @Nonnull ImmIndexSubset<T>
    immutableSubset(@Nonnull Predicate<? super T> p) {
        return new SimpleImmIndexSubset<>(parent, subtract(parent, bs.copy(), p.negate()));
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull T value) {
        Bitset bs = BitsetOps.subset(parent, this, this.bs, value);
        return new SimpleIndexSubset<>(parent, bs);
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull T value) {
        Bitset bs = BitsetOps.subset(parent, this, this.bs, value);
        return new SimpleImmIndexSubset<>(parent, bs);
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableFullSubset() {
        return SimpleImmIndexSubset.createFull(parent);
    }

    @Nonnull @Override public ImmIndexSubset<T> immutableEmptySubset() {
        return SimpleImmIndexSubset.createEmpty(parent);
    }

    @Override public boolean containsAny(@Nonnull Collection<?> c) {
        Bitset rBS = BitsetOps.getBitset(parent, c);
        if (rBS != null)
            return bs.intersects(rBS);
        for (Object o : c) {
            @SuppressWarnings("SuspiciousMethodCalls") int i = parent.indexOf(o);
            if (i >= 0 || bs.get(i)) return true;
        }
        return false;
    }

    @Override public int hash(@Nonnull T elem) {
        return parent.hash(elem);
    }

    /* --- --- Implement Object methods --- --- */

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == parent)
            return size() == parent.size();
        if (o instanceof IndexSet || !(o instanceof Set)) {
            Collection<?> that = (Collection<?>) o;
            return size() == that.size() && containsAll(that);
        } else {
            Set<?> that = (Set<?>) o;
            return that.size() == size() && that.containsAll(this);
        }
    }

    @Override public int hashCode() {
        // do not cache, since getBitSet() can be modified
        int h = 0;
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
            h += parent.hash(parent.get(i));
        return h;
    }

    /* --- --- Implement Collection methods --- --- */

    protected class It implements Iterator<T> {
        int nextIdx, lastIdx = -1;

        @Override
        public boolean hasNext() {
            if (nextIdx >= 0)
                nextIdx = bs.nextSetBit(nextIdx);
            return nextIdx >= 0;
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException("Iterator past the end");
            return parent.get(lastIdx = this.nextIdx++);
        }

        @Override
        public void remove() {
            if (lastIdx < 0)
                throw new IllegalStateException("next() not called yet");
            bs.clear(lastIdx);
        }
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return new It();
    }

    @Override public int size() {
        return bs.cardinality();
    }

    @Override public boolean isEmpty() {
        return bs.isEmpty();
    }

    @Override public boolean contains(Object o) {
        //noinspection SuspiciousMethodCalls
        int idx = parent.indexOf(o);
        return idx >= 0 && bs.get(idx);
    }

    @Override public boolean containsAll(@Nonnull Collection<?> c) {
        if (c instanceof IndexSubset) {
            IndexSubset<?> that = (IndexSubset<?>) c;
            if (that.getParent() == parent)
                return bs.containsAll(that.getBitset());
        } else if (c == parent) {
            return size() == parent.size();
        } else if (size() < c.size()) {
            return false;
        }
        for (Object o : c) {
            if (!contains(o)) return false;
        }
        return true;
    }

    @Override
    public boolean add(T t) {
        int idx = parent.indexOf(t);
        if (idx < 0) throw new NotInParentException(t, parent);
        boolean old = bs.get(idx);
        if (!old)
            bs.set(idx);
        return !old;
    }

    @Override public synchronized int safeAdd(T value) {
        int i = parent.indexOf(value);
        if (i < 0) throw new NotInParentException(value, parent);
        bs.set(i);
        return i;
    }

    @Override public int indexOfAdd(T value) {
        int i = parent.indexOf(value);
        if (i < 0) throw new NotInParentException(value, parent);
        bs.set(i);
        return i;
    }

    @Override public boolean parentAdd(@Nonnull T value) {
        int idx = parent.safeAdd(value);
        boolean missing = !bs.get(idx);
        if (missing)
            bs.set(idx);
        return missing;
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        int old = bs.cardinality();
        BitsetOps.union(parent, bs, c);
        assert bs.cardinality() >= old;
        return bs.cardinality() != old;
    }

    @Override public boolean parentAddAll(@Nonnull Collection<T> values) {
        Bitset ss = BitsetOps.getBitset(parent, values);
        if (ss != null) {
            int old = bs.cardinality();
            bs.or(ss);
            assert old <= bs.cardinality();
            return old != bs.cardinality();
        } else {
            boolean change = false;
            for (T v : values)
                change |= parentAdd(v);
            return change;
        }
    }

    @Override public boolean remove(Object o) {
        //noinspection SuspiciousMethodCalls
        int idx = parent.indexOf(o);
        if (idx < 0) return false;
        boolean old = bs.get(idx);
        if (old)
            bs.set(idx, false);
        return old;
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        int old = bs.cardinality();
        BitsetOps.intersect(parent, bs, c);
        assert bs.cardinality() <= old;
        return old != bs.cardinality();
    }

    @Override public boolean removeAll(@Nonnull Collection<?> c) {
        int old = bs.cardinality();
        BitsetOps.subtract(parent, bs, c);
        assert bs.cardinality() <= old;
        return old != bs.cardinality();
    }

    @Override public boolean removeIf(@Nonnull Predicate<? super T> filter) {
        int old = bs.cardinality();
        subtract(parent, bs, filter);
        assert bs.cardinality() <= old;
        return bs.cardinality() != old;
    }

    @Override public void clear() {
        bs.clear();
    }

    /* --- --- Implement List methods --- --- */

    @Override public T set(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(int index, @Nonnull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override public T remove(int index) {
        int bitIndex = getBitIndex(index);
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("Index "+ index +" out of bounds");
        boolean old = bs.get(bitIndex);
        bs.clear(bitIndex);
        return old ? parent.get(bitIndex) : null;
    }

    @Override public void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException();
    }

    @Override public int indexOf(Object o) {
        if (o == null) return -1;
        int index = 0;
        for (Iterator<T> it = iterator(); it.hasNext(); ) {
            if (it.next().equals(o))
                return index;
            ++index;
        }
        return -1; // not found
    }

    @Override public int lastIndexOf(Object o) {
        return indexOf(o);
    }

    protected class  ListIt implements ListIterator<T> {
        int cursor, lastBit = -1, nextBit;

        public ListIt(int cursor) {
            this.cursor = cursor;
            this.nextBit = getBitIndex(cursor);
        }

        @Override public boolean hasPrevious() { return cursor > 0; }
        @Override public int previousIndex() { return cursor - 1; }

        @Override public T previous() {
            if (!hasPrevious()) throw new NoSuchElementException();
            assert lastBit > 0;
            lastBit  = bs.previousSetBit(lastBit - 1);
            assert lastBit >= 0;
            return parent.get(lastBit);
        }

        @Override public int nextIndex() { return cursor; }

        @Override public boolean hasNext() {
            return nextBit >= 0 || (nextBit = bs.nextSetBit(lastBit + 1)) >= 0;
        }

        @Override public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            T value = parent.get(lastBit = nextBit);
            nextBit = -1;
            return value;
        }

        @Override public void remove() {
            if (lastBit < 0) throw new IllegalStateException("next()/previous() not yet called");
            bs.clear(lastBit);
        }

        @Override public void set(T t) {
            throw new UnsupportedOperationException("IndexedSubset cannot control the order");
        }

        @Override public void add(T t) {
            throw new UnsupportedOperationException("IndexedSubset cannot control the order");
        }
    }

    @Override public @Nonnull ListIterator<T> listIterator() {
        return new ListIt(0);
    }

    @Override public @Nonnull ListIterator<T> listIterator(int index) {
        return new ListIt(index);
    }

    @Override public @Nonnull List<T> subList(int fromIndex, int toIndex) {
        int size = size();
        checkPositionIndex(fromIndex, size);
        checkPositionIndex(toIndex, size +1);
        int subSize = toIndex - fromIndex;
        ArrayList<T> list = new ArrayList<>(subSize);
        for (ListIterator<T> it = listIterator(fromIndex); list.size() < subSize; ) {
            if (!it.hasNext()) {
                assert false : "ListIterator is wrong and missed elements";
                return list;
            }
            list.add(it.next());
        }
        return list;
    }

    @Override public T get(int index) {
        int bitIndex = getBitIndex(index);
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("index="+index+", size="+size());
        return parent.get(bitIndex);
    }

    /* --- --- internal methods --- --- */

    private int getBitIndex(int index) {
        int bitIndex = bs.nextSetBit(0);
        for (int i = 0; bitIndex >= 0 && i < index; i++)
            bitIndex = bs.nextSetBit(bitIndex+1);
        return bitIndex;
    }
}
