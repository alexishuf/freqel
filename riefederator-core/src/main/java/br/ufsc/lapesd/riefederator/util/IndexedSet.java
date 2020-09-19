package br.ufsc.lapesd.riefederator.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Spliterator.*;

@Immutable
public class IndexedSet<T> extends AbstractCollection<T> implements List<T>, Set<T> {
    @SuppressWarnings("Immutable")
    protected final @Nonnull List<T> data;
    @SuppressWarnings("Immutable")
    protected final @Nonnull Map<T, Integer> indexMap;
    protected @LazyInit int hash = 0;

    private static final @Nonnull IndexedSet<?> EMPTY
            = new IndexedSet<>(Collections.emptyList(), ImmutableMap.of());

    protected static @Nonnull <U> Map<U, Integer>
    createIndexMap(@Nonnull Collection<U> list) {
        HashMap<U, Integer> map = Maps.newHashMapWithExpectedSize(list.size());
        int idx = 0;
        for (U obj : list)
            map.put(obj, idx++);
        return map;
    }

    public IndexedSet(@Nonnull Collection<T> collection) {
        indexMap = createIndexMap(data = new ArrayList<>(new HashSet<>(collection)));
    }

    protected IndexedSet(@Nonnull List<T> data, @Nonnull Map<T, Integer> indexMap) {
        this.data = data;
        this.indexMap = indexMap;
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> empty() {
        //noinspection unchecked
        return (IndexedSet<U>)EMPTY;
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> fromMap(@Nonnull Map<U, Integer> map,
                                                     @Nonnull List<U> list) {
        if (IndexedSet.class.desiredAssertionStatus()) {
            checkArgument(map.isEmpty() || map.values().stream().max(Integer::compareTo).orElse(0)
                          < list.size(), "Indices past the end of list");
            checkArgument(map.values().stream().noneMatch(i -> i < 0),
                          "Negative indices not allowed");
            checkArgument(list.stream().allMatch(map::containsKey),
                          "There are elements in list that are not keys in the map");
            for (int i = 0; i < list.size(); i++) {
                checkArgument(map.get(list.get(i)) == i, "List item at position "+i
                        +" does not match its index in map");
            }
        }
        return new IndexedSet<>(list, map);
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> fromDistinct(@Nonnull Collection<U> collection) {
        if (collection instanceof IndexedSet)
            return (IndexedSet<U>) collection;
        if (collection.isEmpty())
            return empty();
        if (IndexedSet.class.desiredAssertionStatus())
            checkArgument(new HashSet<>(collection).size() == collection.size());
        Map<U, Integer> indexMap = createIndexMap(collection);
        if (collection instanceof List)
            return new IndexedSet<>((List<U>)collection, indexMap);
        else
            return new IndexedSet<>(new ArrayList<>(collection), indexMap);
    }

    public static @Nonnull <U> IndexedSet<U> fromDistinct(@Nonnull Iterator<U> it) {
        if (!it.hasNext())
            return empty();
        ArrayList<U> list = new ArrayList<>();
        it.forEachRemaining(list::add);
        return new IndexedSet<>(list, createIndexMap(list));
    }


    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> fromDistinctCopy(@Nonnull Collection<U> collection) {
        if (collection.isEmpty())
            return empty();
        if (IndexedSet.class.desiredAssertionStatus())
            checkArgument(new HashSet<>(collection).size() == collection.size());
        return new IndexedSet<>(new ArrayList<>(collection), createIndexMap(collection));
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexedSet<U> from(@Nonnull Collection<U> collection) {
        if (collection.isEmpty())
            return empty();
        return collection instanceof Set ? fromDistinct(collection)
                                         : fromDistinct(new LinkedHashSet<>(collection));
    }

    @CheckReturnValue @SafeVarargs
    public static @Nonnull <U> IndexedSet<U> newIndexedSet(U... values) {
        return from(Arrays.asList(values));
    }

    public @Nonnull IndexedSet<T> createMutation(BiConsumer<Map<T, Integer>, List<T>> consumer) {
        HashMap<T, Integer> indexMap = new HashMap<>(this.indexMap);
        ArrayList<T> data = new ArrayList<>(this.data);
        consumer.accept(indexMap, data);
        return fromMap(indexMap, data);
    }

    protected class EntrySetIt<T> implements Iterator<Map.Entry<T, Integer>> {
        protected Iterator<Map.Entry<T, Integer>> it;

        public EntrySetIt(@Nonnull Iterator<Map.Entry<T, Integer>> it) { this.it = it; }
        @Override public boolean hasNext() { return it.hasNext(); }
        @Override public Map.Entry<T, Integer> next() { return it.next(); }
    }

    public @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator() {
        return new EntrySetIt<>(indexMap.entrySet().iterator());
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
        if (collection instanceof IndexedSubset
                && ((IndexedSubset<?>) collection).getParent() == this) {
            //noinspection unchecked
            return (IndexedSubset<T>)collection;
        }
        BitSet bitSet = new BitSet(size());
        for (T o : collection) {
            int idx = indexOf(o);
            if (idx >= 0)
                bitSet.set(idx);
        }
        return new IndexedSubset<>(this, bitSet);
    }

    @CheckReturnValue @SafeVarargs
    public @Nonnull final IndexedSubset<T> subsetWith(@Nonnull T... array) {
        return subset(Arrays.asList(array));
    }

    @CheckReturnValue
    public @Nonnull IndexedSubset<T> subset(@Nonnull Predicate<? super T> predicate) {
        BitSet bitSet = new BitSet(size());
        for (int i = 0; i < size(); i++) {
            if (predicate.test(get(i)))
                bitSet.set(i);
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

    @CheckReturnValue
    public @Nonnull IndexedSetPartition<T> partition(int begin, int end) {
        return IndexedSetPartition.from(this, begin, end);
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

    public boolean containsAny(@Nonnull Collection<?> c) {
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

    int hash(@Nonnull T object) {
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
        if (o instanceof IndexedSet && o.hashCode() != hashCode())
            return false; //quicker since IndexedSet caches the hash
        return containsAll(that);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int local = 0;
            for (T e : data)
                local += hash(e);
            hash = local;
        }
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
        return Spliterators.spliterator(iterator(), data.size(),
                DISTINCT|NONNULL|SIZED|ORDERED);
    }
}
