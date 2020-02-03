package br.ufsc.lapesd.riefederator.util;


import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Collection;
import java.util.function.Predicate;

@Immutable
@SuppressWarnings("Immutable")
public class ImmutableIndexedSubset<T> extends IndexedSubset<T> {
    private int hash = 0;

    public ImmutableIndexedSubset(@Nonnull IndexedSet<T> parent, @Nonnull BitSet bitSet) {
        super(parent, bitSet);
    }

    public static @Nonnull <U> ImmutableIndexedSubset<U> copyOf(@Nonnull IndexedSubset<U> subset) {
        BitSet bitSet = new BitSet(subset.getParent().size());
        bitSet.or(subset.getBitSet());
        return new ImmutableIndexedSubset<>(subset.getParent(), bitSet);
    }

    public static @Nonnull <U> ImmutableIndexedSubset<U> empty() {
        assert emptyBitSet.cardinality() == 0;
        assert emptyBitSet.size() == 0;
        return new ImmutableIndexedSubset<>(IndexedSet.empty(), emptyBitSet);
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = super.hashCode();
        return hash;
    }

    @Override
    public void intersect(@Nonnull Collection<? extends T> o) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }

    @Override
    public int union(@Nonnull Collection<? extends T> o) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends T> c) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException("ImmutableIndexedSubset");
    }
}
