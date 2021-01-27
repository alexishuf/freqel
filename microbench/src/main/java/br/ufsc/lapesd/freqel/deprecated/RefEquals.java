package br.ufsc.lapesd.freqel.deprecated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RefEquals<T> {
    private final @Nonnull T obj;

    public RefEquals(@Nonnull T obj) {
        this.obj = obj;
    }
    public static @Nonnull <T> RefEquals<T> of(@Nonnull T obj) {
        return new RefEquals<>(obj);
    }

    public @Nonnull T get() {
        return obj;
    }

    public boolean isSame(@Nullable Object o) {
        return obj == (o instanceof RefEquals ? ((RefEquals<?>) o).obj : o);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass") @Override
    public boolean equals(Object o) {
        return isSame(o);
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(obj);
    }

    @Override
    public String toString() {
        return obj.toString();
    }
}
