package br.ufsc.lapesd.riefederator.util.indexed;

import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Predicate;

import static java.util.Spliterator.*;

public interface IndexSet<T> extends List<T>, Set<T> {

    @Nonnull IndexSet<T> getParent();

    @Nonnull ImmIndexSet<T> asImmutable();

    @Nonnull ImmIndexSet<T> immutableCopy();

    @Nonnull IndexSet<T> copy();

    @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator();

    @CheckReturnValue @Nonnull IndexSubset<T> fullSubset();

    @CheckReturnValue @Nonnull IndexSubset<T> emptySubset();

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull Collection<? extends T> collection);

    @CheckReturnValue @Nonnull IndexSubset<T> subsetExpanding(@Nonnull Collection<? extends T> collection);

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull Bitset subset);

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate);

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull T value);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableFullSubset();

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableEmptySubset();

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Collection<? extends T> collection);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubsetExpanding(@Nonnull Collection<? extends T> collection);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Bitset subset);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Predicate<? super T> predicate);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull T value);

    boolean containsAny(@Nonnull Collection<?> c);

    /**
     * Add a value, if not already present, and return its index within the IndexSet.
     *
     * Unlike {@link Collection#add(Object)}, this method is thread-safe <b>at least</b> among
     * invocations of this particular method. Implementations need not ensure thread safety
     * between invocations to this method and {@link Collection#add(Object)}.
     *
     * @param value object to add (if not already present)
     * @return index of the (existing or new) value
     */
    int safeAdd(T value);

    int hash(@Nonnull T elem);

    @Override default Spliterator<T> spliterator() {
        return Spliterators.spliterator(iterator(), size(),
                DISTINCT|NONNULL|SIZED|ORDERED);
    }
}
