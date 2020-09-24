package br.ufsc.lapesd.riefederator.util.indexed;

import br.ufsc.lapesd.riefederator.util.ref.IdentityHashSet;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import java.util.*;

public class FullIndexSet<T> extends BaseIndexSet<T> {

    public FullIndexSet(int capacity) {
        this(Maps.newHashMapWithExpectedSize(capacity), new ArrayList<>(capacity));
    }

    public FullIndexSet(@Nonnull Map<T, Integer> indexMap, @Nonnull List<T> data) {
        super(indexMap, data);
    }

    protected static @Nonnull <U> Map<U, Integer>
    createIndexMap(@Nonnull Collection<U> list) {
        HashMap<U, Integer> map = Maps.newHashMapWithExpectedSize(list.size());
        int idx = 0;
        for (U obj : list)
            map.put(obj, idx++);
        return map;
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexSet<U> fromDistinct(@Nonnull Collection<U> collection) {
        if (collection instanceof IndexSet)
            return (IndexSet<U>) collection;
        if (collection.isEmpty())
            return ImmFullIndexSet.empty();
        assert new IdentityHashSet<>(collection).size() == collection.size()
                : "Duplicate instances in collection";
        assert new HashSet<>(collection).size() == collection.size()
                : "Duplicate values in collection";
        Map<U, Integer> indexMap = createIndexMap(collection);
        if (collection instanceof List)
            return new FullIndexSet<>(indexMap, (List<U>)collection);
        else
            return new FullIndexSet<>(indexMap, new ArrayList<>(collection));
    }

    public static @Nonnull <U> IndexSet<U> fromDistinct(@Nonnull Iterator<U> it) {
        if (!it.hasNext())
            return ImmFullIndexSet.empty();
        ArrayList<U> list = new ArrayList<>();
        it.forEachRemaining(list::add);
        assert new IdentityHashSet<>(list).size() == list.size()
                : "There are duplicate instances in collection";
        assert new HashSet<>(list).size() == list.size() : "Duplicate values in collection";
        return new FullIndexSet<>(createIndexMap(list), list);
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexSet<U> fromDistinctCopy(@Nonnull Collection<U> collection) {
        if (collection.isEmpty())
            return ImmFullIndexSet.empty();
        assert new IdentityHashSet<>(collection).size() == collection.size()
                : "Same instance is repeated in supposedly distinct collection";
        assert new HashSet<>(collection).size() == collection.size()
                : "There are duplicate values (compared by equals()) in distinct collection";
        return new FullIndexSet<>(createIndexMap(collection), new ArrayList<>(collection));
    }

    @CheckReturnValue
    public static @Nonnull <U> IndexSet<U> from(@Nonnull Collection<U> collection) {
        if (collection.isEmpty())
            return ImmFullIndexSet.empty();
        return collection instanceof Set ? fromDistinct(collection)
                                         : fromDistinct(new LinkedHashSet<>(collection));
    }

    public static @Nonnull <U> IndexSet<U> newIndexSet(U... values) {
        ArrayList<U> list = new ArrayList<>(values.length);
        for (U value : values)
            list.add(value);
        return from(list);
    }

    @Override public @Nonnull ImmIndexSet<T> asImmutable() {
        return new ImmFullIndexSet<>(indexMap, data);
    }

    @Override public @Nonnull IndexSet<T> copy() {
        return new FullIndexSet<>(new HashMap<>(indexMap), new ArrayList<>(data));
    }

    @Override public @Nonnull ImmIndexSet<T> immutableCopy() {
        return new ImmFullIndexSet<>(new HashMap<>(indexMap), new ArrayList<>(data));
    }

    @Override public boolean add(T t) {
        if (t == null) throw new NullPointerException();
        boolean change = indexMap.putIfAbsent(t, data.size()) == null;
        if (change)
            data.add(t);
        return change;
    }

    @Override synchronized public int safeAdd(T value) {
        if (value == null) throw new NullPointerException();
        int size = data.size();
        int idx = indexMap.computeIfAbsent(value, k -> size);
        if (idx == size)
            data.add(value);
        return idx;
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        boolean change = false;
        for (T value : c)
            change |= add(value);
        return change;
    }
}
