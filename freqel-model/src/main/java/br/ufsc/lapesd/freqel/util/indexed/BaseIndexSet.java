package br.ufsc.lapesd.freqel.util.indexed;

import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.subset.*;
import com.google.errorprone.annotations.CheckReturnValue;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Predicate;

import static br.ufsc.lapesd.freqel.util.indexed.subset.BitsetOps.union;


public abstract class BaseIndexSet<T> extends AbstractCollection<T> implements IndexSet<T> {
    protected final @Nonnull List<T> data;
    protected final @Nonnull Map<T, Integer> indexMap;

    protected BaseIndexSet(@Nonnull Map<T, Integer> indexMap, @Nonnull List<T> data) {
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

    @Override public @Nonnull IndexSet<T> getParent() {
        return this;
    }

    protected static class EntrySetIt<U> implements Iterator<Map.Entry<U, Integer>> {
        protected Iterator<Map.Entry<U, Integer>> it;

        public EntrySetIt(@Nonnull Iterator<Map.Entry<U, Integer>> it) { this.it = it; }
        @Override public boolean hasNext() { return it.hasNext(); }
        @Override public Map.Entry<U, Integer> next() { return it.next(); }
    }

    @Override public @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator() {
        return new EntrySetIt<>(indexMap.entrySet().iterator());
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> fullSubset() {
        Bitset bitset = Bitsets.create(size());
        bitset.set(0, size());
        return new SimpleIndexSubset<>(this, bitset);
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> emptySubset() {
        return new SimpleIndexSubset<>(this, Bitsets.create(size()));
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> subset(@Nonnull Collection<? extends T> collection) {
        Bitset bs = union(this, Bitsets.create(size()), collection);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override
    public @Nonnull IndexSubset<T> subsetExpanding(@Nonnull Collection<? extends T> c) {
        return new SimpleIndexSubset<>(this, BitsetOps.subsetExpanding(this, c));
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull Bitset subset) {
        if (subset.length() > size())
            throw new NotInParentException(subset, this);
        return new SimpleIndexSubset<>(this, subset);
    }

    @Override @CheckReturnValue
    public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Collection<? extends T> collection) {
        Bitset bs = union(this, Bitsets.createFixed(size()), collection);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubsetExpanding(@Nonnull Collection<? extends T> c) {
        return new SimpleImmIndexSubset<>(this, BitsetOps.subsetExpanding(this, c));
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Bitset subset) {
        if (subset.length() > size())
            throw new NotInParentException(subset, this);
        return new SimpleImmIndexSubset<>(this, subset);
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate) {
        Bitset bs = union(this, Bitsets.create(size()), predicate);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue public @Nonnull ImmIndexSubset<T>
    immutableSubset(@Nonnull Predicate<? super T> predicate) {
        Bitset bs = union(this, Bitsets.createFixed(size()), predicate);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue
    public @Nonnull IndexSubset<T> subset(@Nonnull T value) {
        Bitset bs = union(this, Bitsets.create(size()), value);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override @CheckReturnValue public @Nonnull ImmIndexSubset<T>
    immutableSubset(@Nonnull T value) {
        Bitset bs = union(this, Bitsets.create(size()), value);
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

    @Override public int safeAdd(T value) {
        throw new UnsupportedOperationException();
    }

    @Override public int indexOfAdd(T value) {
        int i = indexOf(value);
        if (i < 0) {
            i = size();
            boolean added = add(value);
            assert indexOfAddPostconditions(added, value, i);
        }
        return i;
    }

    private boolean indexOfAddPostconditions(boolean added, T value, int i) {
        assert added;
        assert indexOf(value) == i;
        assert size() == i+1;
        return true;
    }

    @Override
    public boolean addAll(int index, @NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        boolean modified = false;
        for (T e : c)
            modified |= add(e);
        return modified;
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
