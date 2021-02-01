package br.ufsc.lapesd.freqel.util;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class RemoveableEmptySet<T> extends AbstractSet<T> {
    private static final @Nonnull RemoveableEmptySet<?> INSTANCE = new RemoveableEmptySet<>();

    @SuppressWarnings("unchecked") public static @Nonnull <T> RemoveableEmptySet<T> get() {
        return (RemoveableEmptySet<T>) INSTANCE;
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return Collections.emptyIterator();
    }

    @Override public int size() {
        return 0;
    }

    @Override public boolean removeAll(Collection<?> c) {
        return false; // report no change instead of throwing
    }

    @Override public boolean remove(Object o) {
        return false; // report no change instead of throwing
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        return false; // report no change instead of throwing
    }

    @Override public void clear() {
        // do nothing instead of throwing
    }
}
