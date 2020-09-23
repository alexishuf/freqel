package br.ufsc.lapesd.riefederator.util.ref;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class EmptyRefSet<T> extends AbstractSet<T> implements RefSet<T> {
    private static final @Nonnull EmptyRefSet<?> INSTANCE = new EmptyRefSet<>();

    public static @Nonnull <T> RefSet<T> emptySet() {
        //noinspection unchecked
        return (RefSet<T>) INSTANCE;
    }

    @Override public @Nonnull Iterator<T> iterator() {
        return Collections.emptyIterator();
    }

    @Override public int size() {
        return 0;
    }

    @Override public boolean equals(Object o) {
        return o instanceof Collection && ((Collection<?>)o).isEmpty();
    }

    @Override public int hashCode() {
        return 0;
    }

    @Override public boolean isEmpty() {
        return true;
    }

    @Override public boolean contains(Object o) {
        return false;
    }
}
