package br.ufsc.lapesd.riefederator.util.indexed;

import br.ufsc.lapesd.riefederator.util.indexed.subset.*;
import com.google.errorprone.annotations.CheckReturnValue;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Predicate;

import static br.ufsc.lapesd.riefederator.util.indexed.subset.BitSetOps.union;


public abstract class BaseIndexSet<T> extends AbstractCollection<T> implements IndexSet<T> {
    protected final @Nonnull List<T> data;
    protected final @Nonnull Map<T, Integer> indexMap;

    protected BaseIndexSet(@Nonnull List<T> data, @Nonnull Map<T, Integer> indexMap) {
        this.data = data;
        this.indexMap = indexMap;
        checkIndex();
    }

    protected void checkIndex() {
        assert indexMap.values().stream().max(Integer::compareTo).orElse(0) < data.size()
               || indexMap.isEmpty()
                : "Indices past the end of list";
        assert indexMap.values().stream().noneMatch(i -> i < 0)
                : "Negative indices not allowed";
        assert data.stream().allMatch(indexMap::containsKey)
                : "There are elements in list that are not keys in the map";
        for (int i = 0; i < data.size(); i++) {
            assert indexMap.get(data.get(i)) == i
                    : "List item at position "+i+" does not match its index in map";
        }
    }

    @Nonnull List<T> getData() {
        return data;
    }

    @Nonnull Map<T, Integer> getIndexMap() {
        return indexMap;
    }

    /* --- --- implement IndexedSet --- --- */

    protected class EntrySetIt<T> implements Iterator<Map.Entry<T, Integer>> {
        protected Iterator<Map.Entry<T, Integer>> it;

        public EntrySetIt(@Nonnull Iterator<Map.Entry<T, Integer>> it) { this.it = it; }
        @Override public boolean hasNext() { return it.hasNext(); }
        @Override public Map.Entry<T, Integer> next() { return it.next(); }
    }

    @Override public @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator() {
        return new EntrySetIt<>(indexMap.entrySet().iterator());
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> fullSubset() {
        BitSet bitSet = new BitSet(size());
        bitSet.set(0, size());
        return new SimpleIndexSubset<>(this, bitSet);
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> emptySubset() {
        return new SimpleIndexSubset<>(this, new BitSet(size()));
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> subset(@Nonnull Collection<? extends T> collection) {
        BitSet bs = BitSetOps.subset(this, new BitSet(size()), collection);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue
    public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Collection<? extends T> collection) {
        BitSet bs = BitSetOps.subset(this, new BitSet(size()), collection);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate) {
        BitSet bs = union(this, new BitSet(size()), predicate);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue public @Nonnull ImmIndexSubset<T>
    immutableSubset(@Nonnull Predicate<? super T> predicate) {
        BitSet bs = union(this, new BitSet(size()), predicate);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue
    public @Nonnull final IndexSubset<T> subset(@Nonnull T value) {
        BitSet bs = BitSetOps.union(this, new BitSet(size()), value);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue public @Nonnull ImmIndexSubset<T>
    immutableSubset(@Nonnull T value) {
        BitSet bs = BitSetOps.union(this, new BitSet(size()), value);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue
    public @Nonnull ImmIndexSubset<T> immutableFullSubset() {
        return SimpleImmIndexSubset.createFull(this);
    }

    @Override @CheckReturnValue
    public @Nonnull ImmIndexSubset<T> immutableEmptySubset() {
        return SimpleImmIndexSubset.createEmpty(this);
    }

    @Override public boolean containsAny(@Nonnull Collection<?> c) {
        if (c instanceof Set && c.size() > size()) {
            for (T element : data) {
                if (c.contains(element)) return true;
            }
        } else {
            for (Object element : c) {
                if (contains(element)) return true;
            }
        }
        return false;
    }

    @Override public int hash(@Nonnull T object) {
        return object.hashCode();
    }

    /* --- implement object methods --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Collection)) return false;
        Collection<?> that = (Collection<?>) o;
        if (that.size() != size())
            return false;
        if (o instanceof IndexSet && o.hashCode() != hashCode())
            return false; //quicker since IndexedSet caches the hash
        return containsAll(that);
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (T e : data)
            hash += hash(e);
        return hash;
    }

    @Override
    public @Nonnull String toString() {
        if (isEmpty())
            return "{}";
        StringBuilder b = new StringBuilder("{");
        for (T obj : this)
            b.append(obj).append(", ");
        b.setLength(b.length()-2);
        return b.append('}').toString();
    }

    /* --- implement collection methods --- */

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public @Nonnull Iterator<T> iterator() {
        return data.iterator();
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> c) {
        for (Object o : c) {
            if (indexOf(o) < 0) return false;
        }
        return true;
    }

    @Override
    public boolean addAll(int index, @NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T get(int index) {
        return data.get(index);
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        //noinspection SuspiciousMethodCalls
        return indexMap.getOrDefault(o, -1);
    }

    @Override
    public int lastIndexOf(Object o) {
        return indexOf(o);
    }

    @Override
    public @Nonnull ListIterator<T> listIterator() {
        return data.listIterator();
    }

    @Override
    public @Nonnull ListIterator<T> listIterator(int index) {
        return data.listIterator(index);
    }

    @Override
    public @Nonnull List<T> subList(int fromIndex, int toIndex) {
        return data.subList(fromIndex, toIndex);
    }
}
