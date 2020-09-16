package br.ufsc.lapesd.riefederator.util;

import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class RefIndexedSet<T> extends IndexedSet<T> {
    private static final IdentityHashMap<Object, Integer> EMPTY_MAP = new IdentityHashMap<>();

    private static final @Nonnull RefIndexedSet<?> EMPTY =
            new RefIndexedSet<>(Collections.emptyList(), EMPTY_MAP);

    protected RefIndexedSet(@Nonnull List<T> data, @Nonnull IdentityHashMap<T, Integer> indexMap) {
        super(data, indexMap);
    }

    @CheckReturnValue
    public static @Nonnull <U> RefIndexedSet<U> fromMap(@Nonnull IdentityHashMap<U, Integer> map,
                                                        @Nonnull List<U> list) {
        if (RefIndexedSet.class.desiredAssertionStatus()) {
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
        return new RefIndexedSet<>(list, map);
    }

    public static @Nonnull <U> RefIndexedSet<U> empty() {
        //noinspection unchecked
        return (RefIndexedSet<U>)EMPTY;
    }

    private static @Nonnull <U> RefIndexedSet<U> createRefDistinct(@Nonnull Collection<U> coll) {
        List<U> list = coll instanceof List ? (List<U>)coll : new ArrayList<>(coll);
        IdentityHashMap<U, Integer> indexMap = new IdentityHashMap<>(list.size());
        int i = 0;
        for (U u : list) indexMap.put(u, i++);
        return new RefIndexedSet<>(list, indexMap);
    }

    public static @Nonnull <U> RefIndexedSet<U> fromRefDistinct(@Nonnull Collection<U> coll) {
        if (coll instanceof RefIndexedSet)  return (RefIndexedSet<U>) coll;
        else if (coll.isEmpty())         return empty();
        else                             return createRefDistinct(coll);
    }

    public static @Nonnull <U> RefIndexedSet<U> fromRefDistinctCopy(@Nonnull Collection<U> coll) {
        if (coll instanceof RefIndexedSet)  return (RefIndexedSet<U>) coll;
        else if (coll.isEmpty())         return empty();
        else                             return createRefDistinct(new ArrayList<>(coll));
    }

    @Override
    int hash(@Nonnull T object) {
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
