package br.ufsc.lapesd.riefederator.util;

import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.NotInParentException;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.set.UnmodifiableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.Collections.emptySet;

public class CollectionUtils {
    public static @Nonnull <T, I>
    Set<T> intersect(@Nonnull Collection<I> input,
                     @Nonnull Function<I, ? extends Collection<T>> getter,
                     @Nullable AtomicBoolean dropped) {
        boolean drop = false;
        Iterator<I> it = input.iterator();
        Set<T> result = new HashSet<>(it.hasNext() ? getter.apply(it.next()) : emptySet());
        while (it.hasNext()) {
            Collection<T> values = getter.apply(it.next());
            drop |= result.retainAll(values);
            if (dropped != null && !drop && !result.containsAll(values))
                drop = true;
        }
        if (dropped != null) dropped.set(drop);
        return result.isEmpty() ? emptySet() : result;
    }

    public static @Nonnull <T, I>
    Set<T> union(@Nullable Collection<I> input,
                 @Nonnull Function<I, ? extends Collection<T>> getter, int capacity) {
        Set<T> union = new HashSet<>(capacity);
        if (input != null) {
            for (I i : input)
                union.addAll(getter.apply(i));
        }
        return union;
    }

    public static @Nonnull <T, I>
    Set<T> union(@Nullable Collection<I> input,
                 @Nonnull Function<I, ? extends Collection<T>> getter) {
        Set<T> union = null;
        if (input != null) {
            for (I i : input) {
                Collection<T> values = getter.apply(i);
                if (!values.isEmpty()) {
                    if (union == null)
                        union = new HashSet<>(values.size() + 10);
                    union.addAll(values);
                }
            }
        }
        return union == null ? emptySet() : union;
    }

    public static @Nonnull <T, I>
    IndexSubset<T> union(@Nonnull IndexSet<T> index, @Nullable Collection<I> input,
                         @Nonnull Function<I, ? extends Collection<T>> getter) {
        IndexSubset<T> ss = index.subset(Bitsets.createFixed(index.size()));
        if (input == null || input.isEmpty())
            return ss;
        for (I i : input)
            ss.addAll(getter.apply(i));
        return ss;
    }

    public static @Nonnull <T> Set<T> hashUnion(@Nonnull Collection<T> a,
                                                @Nonnull Collection<T> b) {
        int expected = a.size() + b.size();
        if (expected == 0)
            return emptySet();
        Set<T> set = Sets.newHashSetWithExpectedSize(expected);
        set.addAll(a);
        set.addAll(b);
        return set;
    }

    public static @Nonnull <T> Set<T> union(@Nonnull Collection<T> a, @Nonnull Collection<T> b) {
        assert a != null;
        assert b != null;
        try {
            if (a instanceof IndexSubset)
                return ((IndexSubset<T>) a).createUnion(b);
            else if (b instanceof IndexSubset)
                return ((IndexSubset<T>) b).createUnion(a);
        } catch (NotInParentException ignored) { /* build HashSet instead */ }
        return hashUnion(a, b);
    }

    public static @Nonnull <T> Set<T> intersect(@Nonnull Set<T> left, @Nonnull Set<T> right) {
        if (left instanceof IndexSubset)
            return ((IndexSubset<T>) left).createIntersection(right);
        if (right instanceof IndexSubset)
            return ((IndexSubset<T>) right).createIntersection(left);
        int ls = left.size(), rs = right.size();
        int capacity = Math.min(ls, rs);
        if (capacity == 0)
            return emptySet();
        HashSet<T> set = Sets.newHashSetWithExpectedSize(capacity);
        for (T s : left) {
            if (right.contains(s))
                set.add(s);
        }
        return set;
    }

    public static @Nonnull <T> Set<T> intersect(@Nonnull Collection<T> left,
                                                @Nonnull Collection<T> right) {
        boolean leftIsSet = left instanceof Set, rightIsSet = right instanceof Set;

        if (leftIsSet && rightIsSet) {
            return intersect((Set<T>)left, (Set<T>)right);
        }
        int ls = left.size(), rs = right.size();
        int capacity = Math.min(ls, rs);
        if (leftIsSet || (!rightIsSet && ls < rs)) {
            Collection<T> tmp = left; left = right; right = tmp;
        }
        Set<T> set = Sets.newHashSetWithExpectedSize(capacity);
        for (T v : left) {
            if (right.contains(v))
                set.add(v);
        }
        return set;
    }

    public static <T> boolean hasIntersect(@Nullable Collection<T> left,
                                           @Nullable Collection<T> right) {
        left = left == null ? emptySet() : left;
        right = right == null ? emptySet() : right;
        Collection<T> smaller, set;
        if (left.size() < right.size() || !(left instanceof Set)) {
            smaller = left;
            set = right;
        } else {
            smaller = right;
            set = left;
        }
        for (T t : smaller) {
            if (set.contains(t)) return true;
        }
        return false;
    }

    public static @Nonnull <T> Set<T> intersect(@Nonnull Collection<T> a,
                                                @Nonnull Collection<T> b,
                                                @Nonnull Collection<T> c) {
        Set<T> result = new HashSet<>(a);
        result.retainAll(b);
        result.retainAll(c);
        return result;
    }

    public static @Nonnull <T, I>
    Set<T> intersect(@Nullable Collection<I> input,
                     @Nonnull Function<I, ? extends Collection<T>> getter) {
        if (input == null || input.isEmpty())
            return emptySet();
        Iterator<I> it = input.iterator();
        Set<T> set = new HashSet<>(getter.apply(it.next()));
        while (it.hasNext())
            set.retainAll(getter.apply(it.next()));
        return set;
    }

    public static @Nonnull <T> Set<T> setMinus(@Nullable Collection<T> left,
                                               @Nullable Collection<T> right) {
        left  = left  == null ? emptySet() :  left;
        right = right == null ? emptySet() : right;
        if (left instanceof IndexSet)
            return ((IndexSet<T>) left).fullSubset().minus(right);
        HashSet<T> set = new HashSet<>(left);
        set.removeAll(right);
        return set;
    }

    public static @Nonnull <T> Set<T> unmodifiableSet(@Nonnull Collection<T> collection) {
        if (collection instanceof UnmodifiableSet)
            return (UnmodifiableSet<T>)collection;
        if (collection instanceof Set)
            return Collections.unmodifiableSet((Set<T>)collection);
        return Collections.unmodifiableSet(new HashSet<>(collection));
    }
}
