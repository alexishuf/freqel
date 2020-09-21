package br.ufsc.lapesd.riefederator.util;

import com.google.common.collect.Multiset;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.Nonnull;
import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

@SuppressWarnings("NullableProblems")
public class RemoveableEmptyMultiset<T> extends AbstractCollection<T> implements Multiset<T> {
    private static final @Nonnull
    RemoveableEmptyMultiset<?> INSTANCE = new RemoveableEmptyMultiset<>();

    @SuppressWarnings("unchecked") public static @Nonnull <T> RemoveableEmptyMultiset<T> get() {
        return (RemoveableEmptyMultiset<T>) INSTANCE;
    }

    @Override public int count(@Nullable Object element) {
        return 0;
    }

    @Override public int add(@Nullable T element, int occurrences) {
        throw new UnsupportedOperationException();
    }

    @Override public int remove(@Nullable Object element, int occurrences) {
        return 0;
    }

    @Override public int setCount(@Nonnull T element, int count) {
        throw new UnsupportedOperationException();
    }

    @Override public boolean setCount(@Nonnull T element, int oldCount, int newCount) {
        throw new UnsupportedOperationException();
    }

    @Override public Set<T> elementSet() {
        return RemoveableEmptySet.get();
    }

    @Override public Set<Entry<T>> entrySet() {
        return RemoveableEmptySet.get();
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return Collections.emptyIterator();
    }

    @Override public int size() {
        return 0;
    }

    @Override public boolean remove(Object o) {
        return false;
    }
}
