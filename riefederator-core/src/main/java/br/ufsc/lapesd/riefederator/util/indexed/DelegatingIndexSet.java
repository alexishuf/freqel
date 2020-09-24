package br.ufsc.lapesd.riefederator.util.indexed;

import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public abstract class DelegatingIndexSet<T> implements IndexSet<T> {
    protected @Nonnull IndexSet<T> delegate;

    protected DelegatingIndexSet(@Nonnull IndexSet<T> delegate) {
        this.delegate = delegate;
    }

    @Override @Nonnull public ImmIndexSet<T> asImmutable() {
        return delegate.asImmutable();
    }

    @Override @Nonnull public ImmIndexSet<T> immutableCopy() {
        return delegate.immutableCopy();
    }

    @Override @Nonnull public IndexSet<T> copy() {
        return delegate.copy();
    }

    @Override @Nonnull public Iterator<Map.Entry<T, Integer>> entryIterator() {
        return delegate.entryIterator();
    }

    @Override @Nonnull @CheckReturnValue public IndexSubset<T> fullSubset() {
        return delegate.fullSubset();
    }

    @Override @Nonnull @CheckReturnValue public IndexSubset<T> emptySubset() {
        return delegate.emptySubset();
    }

    @Override @Nonnull @CheckReturnValue
    public IndexSubset<T> subset(@Nonnull Collection<? extends T> collection) {
        return delegate.subset(collection);
    }

    @Override
    public @Nonnull IndexSubset<T> subsetExpanding(@Nonnull Collection<? extends T> collection) {
        return delegate.subsetExpanding(collection);
    }

    @Override public @Nonnull IndexSubset<T> subset(@Nonnull BitSet subset) {
        return delegate.subset(subset);
    }

    @Override @Nonnull @CheckReturnValue
    public IndexSubset<T> subset(@Nonnull Predicate<? super T> predicate) {
        return delegate.subset(predicate);
    }

    @Override @Nonnull @CheckReturnValue public IndexSubset<T> subset(@Nonnull T value) {
        return delegate.subset(value);
    }

    @Override @Nonnull @CheckReturnValue public ImmIndexSubset<T> immutableFullSubset() {
        return delegate.immutableFullSubset();
    }

    @Override @Nonnull @CheckReturnValue public ImmIndexSubset<T> immutableEmptySubset() {
        return delegate.immutableEmptySubset();
    }

    @Override @Nonnull @CheckReturnValue
    public ImmIndexSubset<T> immutableSubset(@Nonnull Collection<? extends T> collection) {
        return delegate.immutableSubset(collection);
    }

    @Override
    public @Nonnull ImmIndexSubset<T> immutableSubsetExpanding(@Nonnull Collection<? extends T> c) {
        return delegate.immutableSubsetExpanding(c);
    }

    @Override public @Nonnull ImmIndexSubset<T> immutableSubset(@Nonnull BitSet subset) {
        return delegate.immutableSubset(subset);
    }

    @Override @Nonnull @CheckReturnValue
    public ImmIndexSubset<T> immutableSubset(@Nonnull Predicate<? super T> predicate) {
        return delegate.immutableSubset(predicate);
    }

    @Override @Nonnull @CheckReturnValue
    public ImmIndexSubset<T> immutableSubset(@Nonnull T value) {
        return delegate.immutableSubset(value);
    }

    @Override public boolean containsAny(@Nonnull Collection<?> c) {
        return delegate.containsAny(c);
    }

    @Override public int hash(@Nonnull T elem) {
        return delegate.hash(elem);
    }

    @Override public Spliterator<T> spliterator() {
        return delegate.spliterator();
    }

    @Override public int size() {
        return delegate.size();
    }

    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Nonnull @Override public Iterator<T> iterator() {
        return delegate.iterator();
    }

    @Nonnull @Override public Object[] toArray() {
        return delegate.toArray();
    }

    @Nonnull @Override public <T1> T1[] toArray(@Nonnull T1[] a) {
        //noinspection SuspiciousToArrayCall
        return delegate.toArray(a);
    }

    @Override public boolean add(T t) {
        return delegate.add(t);
    }

    @Override public boolean remove(Object o) {
        return delegate.remove(o);
    }

    @Override public boolean containsAll(@Nonnull Collection<?> c) {
        return delegate.containsAll(c);
    }

    @Override public boolean addAll(@Nonnull Collection<? extends T> c) {
        return delegate.addAll(c);
    }

    @Override public boolean addAll(int index, @Nonnull Collection<? extends T> c) {
        return delegate.addAll(index, c);
    }

    @Override public boolean removeAll(@Nonnull Collection<?> c) {
        return delegate.removeAll(c);
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        return delegate.retainAll(c);
    }

    @Override public void replaceAll(UnaryOperator<T> operator) {
        delegate.replaceAll(operator);
    }

    @Override public void sort(Comparator<? super T> c) {
        delegate.sort(c);
    }

    @Override public void clear() {
        delegate.clear();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass") @Override public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override public int hashCode() {
        return delegate.hashCode();
    }

    @Override public T get(int index) {
        return delegate.get(index);
    }

    @Override public T set(int index, T element) {
        return delegate.set(index, element);
    }

    @Override public void add(int index, T element) {
        delegate.add(index, element);
    }

    @Override public T remove(int index) {
        return delegate.remove(index);
    }

    @Override public int indexOf(Object o) {
        return delegate.indexOf(o);
    }

    @Override public int lastIndexOf(Object o) {
        return delegate.lastIndexOf(o);
    }

    @Nonnull @Override public ListIterator<T> listIterator() {
        return delegate.listIterator();
    }

    @Nonnull @Override public ListIterator<T> listIterator(int index) {
        return delegate.listIterator(index);
    }

    @Nonnull @Override public List<T> subList(int fromIndex, int toIndex) {
        return delegate.subList(fromIndex, toIndex);
    }

    @Override public boolean removeIf(Predicate<? super T> filter) {
        return delegate.removeIf(filter);
    }

    @Override public Stream<T> stream() {
        return delegate.stream();
    }

    @Override public Stream<T> parallelStream() {
        return delegate.parallelStream();
    }

    @Override public void forEach(Consumer<? super T> action) {
        delegate.forEach(action);
    }
}
