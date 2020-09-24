package br.ufsc.lapesd.riefederator.util.indexed.subset;

import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Collection;

public interface IndexSubset<T> extends IndexSet<T> {
    @Nonnull BitSet getBitSet();

    @Override @Nonnull ImmIndexSubset<T> asImmutable();

    @Nonnull @Override ImmIndexSubset<T> immutableCopy();

    @Nonnull @Override IndexSubset<T> copy();

    @Contract("-> this") @Nonnull IndexSubset<T> complement();

    @Contract("_ -> this") @Nonnull IndexSubset<T> intersect(@Nonnull Collection<? extends T> coll);

    @Contract("_ -> this") @Nonnull IndexSubset<T> union(@Nonnull Collection<? extends T> coll);

    @Contract("_ -> this") @Nonnull IndexSubset<T> minus(@Nonnull Collection<? extends T> coll);

    @Contract("_ -> this") @Nonnull IndexSubset<T> symDiff(@Nonnull Collection<? extends T> coll);

    @Nonnull IndexSubset<T> createComplement();

    @Nonnull IndexSubset<T> createIntersection(@Nonnull Collection<? extends T> coll);

    @Nonnull IndexSubset<T> createUnion(@Nonnull Collection<? extends T> collection);

    @Nonnull <U extends T> IndexSubset<T> createUnion(@Nonnull U value);

    @Nonnull IndexSubset<T> createSymDiff(@Nonnull Collection<? extends T> collection);

    @Nonnull ImmIndexSubset<T> createImmutableComplement();

    @Nonnull ImmIndexSubset<T> createImmutableIntersection(@Nonnull Collection<? extends T> coll);

    @Nonnull ImmIndexSubset<T> createImmutableUnion(@Nonnull Collection<? extends T> collection);

    @Nonnull ImmIndexSubset<T> createImmutableSymDiff(@Nonnull Collection<? extends T> coll);

    @Nonnull <U extends T> ImmIndexSubset<T> createImmutableUnion(@Nonnull U value);

    @Nonnull IndexSubset<T> createMinus(@Nonnull Collection<? extends T> coll);

    boolean hasIndex(int idx, @Nonnull IndexSet<T> expectedParent);

    @Nonnull T getAtIndex(int idx, @Nonnull IndexSet<T> expectedParent);

    boolean setIndex(int index, @Nonnull IndexSet<T> parent);

    boolean parentAdd(@Nonnull T value);

    boolean parentAddAll(@Nonnull Collection<T> values);

}
