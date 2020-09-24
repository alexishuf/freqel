package br.ufsc.lapesd.riefederator.util.indexed;

import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.function.Predicate;

public class CoWIndexSet<T> extends DelegatingIndexSet<T> {
    private boolean shared;

    public CoWIndexSet(@Nonnull IndexSet<T> delegate, boolean shared) {
        super(delegate);
        this.shared = shared;
    }

    public static @Nonnull <U> CoWIndexSet<U> shared(@Nonnull IndexSet<U> other) {
        return new CoWIndexSet<>(other, true);
    }

    protected @Nonnull IndexSet<T> exclusive() {
        if (shared) {
            shared = false;
            delegate = delegate.copy();
        }
        return delegate;
    }

    @Override public @Nonnull IndexSet<T> getParent() {
        return this;
    }

    @Override
    public @Nonnull IndexSubset<T> subsetExpanding(@Nonnull Collection<? extends T> c) {
        return exclusive().subsetExpanding(c);
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubsetExpanding(@Nonnull Collection<? extends T> c) {
        return exclusive().immutableSubsetExpanding(c);
    }

    @Override public boolean add(T t) {
        return exclusive().add(t);
    }

    @Override public int safeAdd(T value) {
        return exclusive().safeAdd(value);
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        return exclusive().addAll(c);
    }

    @Override public boolean removeAll(@Nonnull Collection<?> c) {
        return exclusive().removeAll(c);
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        return exclusive().retainAll(c);
    }

    @Override public void clear() {
        exclusive().clear();
    }

    @Override public T set(int index, T element) {
        return exclusive().set(index, element);
    }

    @Override public void add(int index, T element) {
        exclusive().add(index, element);
    }

    @Override public T remove(int index) {
        return exclusive().remove(index);
    }

    @Override public boolean removeIf(Predicate<? super T> filter) {
        return exclusive().removeIf(filter);
    }
}
