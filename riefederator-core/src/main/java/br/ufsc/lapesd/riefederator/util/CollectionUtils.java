package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
    Set<T> union(@Nonnull Collection<I> input,
                 @Nonnull Function<I, ? extends Collection<T>> getter) {
        Set<T> set = new HashSet<>();
        for (I i : input) set.addAll(getter.apply(i));
        return set;
    }

    public static @Nonnull <T> Set<T> union(@Nonnull Collection<T> a, @Nonnull Collection<T> b) {
        HashSet<T> set = new HashSet<>(a.size() + b.size());
        set.addAll(a);
        set.addAll(b);
        return set;
    }

    public static @Nonnull <T> Set<T> intersect(@Nonnull Collection<T> left, Collection<T> right) {
        Set<T> result = new HashSet<>(left.size() < right.size() ? left : right);
        result.retainAll(left.size() < right.size() ? right : left);
        return result;
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
    Set<T> intersect(@Nonnull Collection<I> input,
                     @Nonnull Function<I, ? extends Collection<T>> getter) {
        if (input.isEmpty())
            return emptySet();
        Iterator<I> it = input.iterator();
        Set<T> set = new HashSet<>(getter.apply(it.next()));
        while (it.hasNext())
            set.retainAll(getter.apply(it.next()));
        return set;
    }

    public static @Nonnull <T> Set<T> setMinus(@Nonnull Collection<T> left,
                                               @Nonnull Collection<T> right) {
        HashSet<T> set = new HashSet<>(left);
        set.removeAll(right);
        return set;
    }
}
