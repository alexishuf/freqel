package br.ufsc.lapesd.riefederator.util.indexed.subset;

import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.function.Predicate;

@Immutable
public class SimpleImmIndexSubset<T> extends SimpleIndexSubset<T>
                                        implements ImmIndexSubset<T> {
    private static final @Nonnull BitSet EMPTY_BITSET = new BitSet(0);

    private @LazyInit int hash;

    public SimpleImmIndexSubset(@Nonnull IndexSet<T> parent, @Nonnull BitSet bitSet) {
        super(parent, bitSet);
    }

    public static @Nonnull <U> SimpleImmIndexSubset<U>
    createEmpty(@Nonnull IndexSet<U> parent) {
        return new SimpleImmIndexSubset<>(parent, EMPTY_BITSET);
    }

    public static @Nonnull <U> SimpleImmIndexSubset<U>
    createFull(@Nonnull IndexSet<U> parent) {
        BitSet bs = new BitSet();
        bs.set(0, parent.size());
        return new SimpleImmIndexSubset<>(parent, bs);
    }

    public static @Nonnull <U> SimpleImmIndexSubset<U>
    create(@Nonnull IndexSet<U> parent, @Nonnull Collection<? extends  U> collection) {
        BitSet bs = BitSetOps.union(parent, new BitSet(), collection);
        return new SimpleImmIndexSubset<>(parent, bs);
    }

    public static @Nonnull <U> SimpleImmIndexSubset<U>
    create(@Nonnull IndexSet<U> parent, @Nonnull Predicate<? super   U> predicate) {
        BitSet bs = BitSetOps.union(parent, new BitSet(), predicate);
        return new SimpleImmIndexSubset<>(parent, bs);
    }

    /* --- --- Implement IndexedSubset methods --- --- */

    @Override public @Nonnull ImmIndexSubset<T> asImmutable() {
        return this;
    }

    /* --- --- Implement IndexedSet methods --- --- */

    /* --- --- Implement Object methods --- --- */

    @Override public int hashCode() {
        return hash == 0 ? (hash = super.hashCode()) : hash;
    }

    /* --- --- Implement Collection methods --- --- */

    protected class ImmutableIt extends It {
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return new ImmutableIt();
    }

    @Override public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean removeAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean removeIf(@Nonnull Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override public void clear() {
        throw new UnsupportedOperationException();
    }

    /* --- --- Implement List methods --- --- */

    @Override public T remove(int index) {
        throw new UnsupportedOperationException();
    }

    protected class ImmutableListIt extends ListIt {
        public ImmutableListIt(int cursor) { super(cursor); }
        @Override public void remove() { throw new UnsupportedOperationException(); }
    }

    @Override public @Nonnull ListIterator<T> listIterator() {
        return new ImmutableListIt(0);
    }

    @Override public @Nonnull ListIterator<T> listIterator(int index) {
        return new ImmutableListIt(index);
    }
}
