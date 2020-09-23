package br.ufsc.lapesd.riefederator.util.indexed;

import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Predicate;

import static java.util.Spliterator.*;

public interface IndexSet<T> extends List<T>, Set<T> {

    @Nonnull ImmIndexSet<T> asImmutable();

    @Nonnull Iterator<Map.Entry<T, Integer>> entryIterator();

    @CheckReturnValue @Nonnull IndexSubset<T> fullSubset();

    @CheckReturnValue @Nonnull IndexSubset<T> emptySubset();

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull Collection<? extends T> collection);

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate);

    @CheckReturnValue @Nonnull IndexSubset<T> subset(@Nonnull T value);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableFullSubset();

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableEmptySubset();

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Collection<? extends T> collection);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Predicate<? super T> predicate);

    @CheckReturnValue @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull T value);

    boolean containsAny(@Nonnull Collection<?> c);

    int hash(@Nonnull T elem);

    @Override default Spliterator<T> spliterator() {
        return Spliterators.spliterator(iterator(), size(),
                DISTINCT|NONNULL|SIZED|ORDERED);
    }
}
