package br.ufsc.lapesd.riefederator.util.indexed.ref;

import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ImmIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import java.util.*;

public class RefIndexSet<T> extends FullIndexSet<T> {
    private static final IdentityHashMap<Object, Integer> EMPTY_MAP = new IdentityHashMap<>();

    private static final @Nonnull RefIndexSet<?> EMPTY =
            new RefIndexSet<>(EMPTY_MAP, Collections.emptyList());

    public RefIndexSet(int capacity) {
        super(new IdentityHashMap<>((int)(capacity/0.75f + 1f)), new ArrayList<>(capacity));
    }

    public RefIndexSet(@Nonnull IdentityHashMap<T, Integer> indexMap, @Nonnull List<T> data) {
        super(indexMap, data);
    }

    public static @Nonnull <U> RefIndexSet<U> empty() {
        //noinspection unchecked
        return (RefIndexSet<U>)EMPTY;
    }

    private static @Nonnull <U> RefIndexSet<U> createRefDistinct(@Nonnull Collection<U> coll) {
        List<U> list = coll instanceof List ? (List<U>)coll : new ArrayList<>(coll);
        IdentityHashMap<U, Integer> indexMap = new IdentityHashMap<>(list.size());
        int i = 0;
        for (U u : list) indexMap.put(u, i++);
        return new RefIndexSet<>(indexMap, list);
    }

    public static @Nonnull <U> RefIndexSet<U> fromRefDistinct(@Nonnull Collection<U> coll) {
        if (coll instanceof RefIndexSet)  return (RefIndexSet<U>) coll;
        else if (coll.isEmpty())         return empty();
        else                             return createRefDistinct(coll);
    }

    public static @Nonnull <U> RefIndexSet<U> fromRefDistinctCopy(@Nonnull Collection<U> coll) {
        if (coll instanceof RefIndexSet)  return (RefIndexSet<U>) coll;
        else if (coll.isEmpty())         return empty();
        else                             return createRefDistinct(new ArrayList<>(coll));
    }

    @Override public @Nonnull ImmIndexSet<T> asImmutable() {
        return new ImmRefIndexSet<>((IdentityHashMap<T, Integer>) indexMap, data);
    }

    @Override public @Nonnull IndexSet<T> copy() {
        return new RefIndexSet<>(new IdentityHashMap<>(indexMap), new ArrayList<>(data));
    }

    @Override public @Nonnull ImmIndexSet<T> immutableCopy() {
        return new ImmRefIndexSet<>(new IdentityHashMap<>(indexMap), new ArrayList<>(data));
    }

    @Override
    public int hash(@Nonnull T object) {
        return System.identityHashCode(object);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass") @Override
    public boolean equals(Object o) {
        return indexMap.keySet().equals(o);
    }

    @Override
    public int hashCode() {
        return indexMap.keySet().hashCode();
    }
}
