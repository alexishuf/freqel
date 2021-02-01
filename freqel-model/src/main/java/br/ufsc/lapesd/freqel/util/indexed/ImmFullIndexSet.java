package br.ufsc.lapesd.freqel.util.indexed;

import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.subset.*;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@Immutable
public class ImmFullIndexSet<T> extends FullIndexSet<T> implements ImmIndexSet<T> {
    private static final @Nonnull IndexSet<?> EMPTY
            = new FullIndexSet<>(ImmutableMap.of(), Collections.emptyList());
    private @LazyInit int hash = 0;

    protected ImmFullIndexSet(@Nonnull Map<T, Integer> indexMap, @Nonnull List<T> data) {
        super(indexMap, data);
    }

    @CheckReturnValue
    public static @Nonnull <U> FullIndexSet<U> empty() {
        //noinspection unchecked
        return (FullIndexSet<U>)EMPTY;
    }

    @Override public @Nonnull IndexSubset<T> fullSubset() {
        Bitset bs = Bitsets.createFixed(size());
        bs.set(0, size());
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override public @Nonnull IndexSubset<T> emptySubset() {
        return new SimpleIndexSubset<>(this, Bitsets.createFixed(size()));

    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull Collection<? extends T> collection) {
        Bitset bs = BitsetOps.union(this, Bitsets.createFixed(size()), collection);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull T value) {
        Bitset bs = BitsetOps.union(this, Bitsets.createFixed(size()), value);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override public @Nonnull IndexSubset<T> subsetExpanding(@Nonnull Collection<? extends T> c) {
        return subset(c);
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Collection<? extends T> collection) {
        Bitset bs = BitsetOps.union(this, Bitsets.createFixed(size()), collection);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubsetExpanding(@Nonnull Collection<? extends T> c) {
        return immutableSubset(c);
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate) {
        Bitset bs = BitsetOps.union(this, Bitsets.createFixed(size()), predicate);
        return new SimpleIndexSubset<>(this, bs);
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull Predicate<? super T> predicate) {
        Bitset bs = BitsetOps.union(this, Bitsets.createFixed(size()), predicate);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull T value) {
        Bitset bs = BitsetOps.union(this, Bitsets.createFixed(size()), value);
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableFullSubset() {
        Bitset bs = Bitsets.createFixed(size());
        bs.set(0, size());
        return new SimpleImmIndexSubset<>(this, bs);
    }

    @Override public @Nonnull ImmIndexSet<T> asImmutable() {
        return this;
    }

    @Override public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override public int hashCode() {
        assert hash == 0 || hash == super.hashCode() : "Cached hash code became invalid!";
        return hash == 0 ? (hash = super.hashCode()) : hash;
    }
}
