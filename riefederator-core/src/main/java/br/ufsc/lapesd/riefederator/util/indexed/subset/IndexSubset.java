package br.ufsc.lapesd.riefederator.util.indexed.subset;

import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Collection;

public interface IndexSubset<T> extends IndexSet<T> {
    @Nonnull IndexSet<T> getParent();

    @Nonnull BitSet getBitSet();

    @Nonnull ImmIndexSubset<T> asImmutable();

    void complement();

    @Nonnull IndexSubset<T> copy();

    @Nonnull ImmIndexSubset<T> immutableCopy();

    @Nonnull IndexSubset<T> createComplement();

    @Nonnull IndexSubset<T> createIntersection(@Nonnull Collection<? extends T> coll);

    @Nonnull IndexSubset<T> createUnion(@Nonnull Collection<? extends T> collection);

    @Nonnull <U extends T> IndexSubset<T> createUnion(@Nonnull U value);

    @Nonnull ImmIndexSubset<T> createImmutableComplement();

    @Nonnull ImmIndexSubset<T> createImmutableIntersection(@Nonnull Collection<? extends T> coll);

    @Nonnull ImmIndexSubset<T> createImmutableUnion(@Nonnull Collection<? extends T> collection);

    @Nonnull <U extends T> ImmIndexSubset<T> createImmutableUnion(@Nonnull U value);

    @Nonnull IndexSubset<T> createDifference(@Nonnull Collection<? extends T> coll);

    boolean hasIndex(int idx, @Nonnull IndexSet<T> expectedParent);

    @Nonnull T getAtIndex(int idx, @Nonnull IndexSet<T> expectedParent);

    boolean setIndex(int index, @Nonnull IndexSet<T> parent);

}
