package br.ufsc.lapesd.riefederator.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.Spliterator.*;

@Immutable
public class IndexedSet<T> extends AbstractCollection<T> implements List<T>, Set<T> {
    @SuppressWarnings("Immutable")
    private final @Nonnull List<T> data;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableMap<T, Integer> indexMap;
    private @LazyInit int hash = 0;

    protected static @Nonnull <U> ImmutableMap<U, Integer>
    createIndexMap(@Nonnull Collection<U> list) {
        ImmutableMap.Builder<U, Integer> b;
        //noinspection UnstableApiUsage
        b = ImmutableMap.builderWithExpectedSize(list.size());
        int idx = 0;
        for (U obj : list)
            b.put(obj, idx++);
        return b.build();
    }

    public IndexedSet(@Nonnull Collection<T> collection) {
        indexMap = createIndexMap(data = new ArrayList<>(new HashSet<>(collection)));
    }

    protected IndexedSet(@Nonnull List<T> data, @Nonnull ImmutableMap<T, Integer> indexMap) {
        this.data = data;
        this.indexMap = indexMap;
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> empty() {
        return fromDistinct(Collections.emptySet());
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> fromDistinct(@Nonnull Collection<U> collection) {
        if (IndexedSet.class.desiredAssertionStatus())
            Preconditions.checkArgument(new HashSet<>(collection).size() == collection.size());
        ImmutableMap<U, Integer> indexMap = createIndexMap(collection);
        if (collection instanceof List)
            return new IndexedSet<>((List<U>)collection, indexMap);
        else
            return new IndexedSet<>(new ArrayList<>(collection), indexMap);
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> fromDistinctCopy(@Nonnull Collection<U> collection) {
        if (IndexedSet.class.desiredAssertionStatus())
            Preconditions.checkArgument(new HashSet<>(collection).size() == collection.size());
        return new IndexedSet<>(new ArrayList<>(collection), createIndexMap(collection));
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> from(@Nonnull Collection<U> collection) {
        return fromDistinct(new LinkedHashSet<>(collection));
    }

    @CheckReturnValue
    public @Nonnull IndexedSubset<T> fullSubset() {
        BitSet bitSet = new BitSet(size());
        bitSet.set(0, size());
        return new IndexedSubset<>(this, bitSet);
    }

    @CheckReturnValue
    public @Nonnull IndexedSubset<T> emptySubset() {
        return new IndexedSubset<>(this, new BitSet(size()));
    }

    @CheckReturnValue
    public @Nonnull IndexedSubset<T> subset(@Nonnull Collection<? extends T> collection) {
        BitSet bitSet = new BitSet(size());
        for (T o : collection) {
            int idx = indexOf(o);
            if (idx >= 0)
                bitSet.set(idx);
        }
        return new IndexedSubset<>(this, bitSet);
    }

    @CheckReturnValue
    public @Nonnull final IndexedSubset<T> subset(@Nonnull T value) {
        return subset(Collections.singletonList(value));
    }

    @CheckReturnValue
    public @Nonnull ImmutableIndexedSubset<T> fullImmutableSubset() {
        BitSet bitSet = new BitSet(size());
        bitSet.set(0, size());
        return new ImmutableIndexedSubset<>(this, bitSet);
    }

    @CheckReturnValue
    public @Nonnull ImmutableIndexedSubset<T> immutableEmptySubset() {
        return new ImmutableIndexedSubset<>(this, new BitSet(size()));
    }

    @CheckReturnValue
    public @Nonnull ImmutableIndexedSubset<T>
    immutableSubset(@Nonnull Collection<? extends T> collection) {
        BitSet bitSet = new BitSet(size());
        for (T o : collection) {
            int idx = indexOf(o);
            if (idx >= 0)
                bitSet.set(idx);
        }
        return new ImmutableIndexedSubset<>(this, bitSet);
    }

    @CheckReturnValue
    public @Nonnull final ImmutableIndexedSubset<T> immutableSubset(@Nonnull T value) {
        return immutableSubset(Collections.singletonList(value));
    }

    /* --- implement object methods --- */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Collection)) return false;
        Collection<?> that = (Collection<?>) o;
        if (that.size() != size())
            return false;
        if (o instanceof IndexedSet && o.hashCode() != hashCode())
            return false; //quicker since IndexedSet caches the hash
        return containsAll(that);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int[] codes = new int[size()];
            for (int i = 0; i < data.size(); i++)
                codes[i] = Objects.hashCode(data.get(i));
            Arrays.sort(codes);
            HashCodeBuilder b = new HashCodeBuilder();
            for (int code : codes) b.append(code);
            hash = b.toHashCode();
        }
        return hash;
    }

    @Override
    public @Nonnull String toString() {
        if (isEmpty())
            return "{}";
        ArrayList<T> ordered = new ArrayList<>(data);
        ordered.sort(Comparator.comparing(Objects::hashCode));
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (T obj : ordered)
            b.append(obj).append(", ");
        b.setLength(b.length()-2);
        return b.append('}').toString();
    }

    /* --- implement collection methods --- */

    @Override
    public @Nonnull Iterator<T> iterator() {
        return data.iterator();
    }

    @Override
    public boolean addAll(int index, @NotNull Collection<? extends T> c) {
        throw new UnsupportedOperationException("IndexedSet is Immutable");
    }

    @Override
    public T get(int index) {
        return data.get(index);
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException("IndexedSet is Immutable");
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException("IndexedSet is Immutable");
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException("IndexedSet is Immutable");
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

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public @Nonnull Spliterator<T> spliterator() {
        return Spliterators.spliterator(data.iterator(), data.size(),
                DISTINCT|NONNULL|SIZED|ORDERED);
    }
}
