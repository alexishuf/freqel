package br.ufsc.lapesd.riefederator;

import javax.annotation.Nonnull;
import java.util.function.Function;

public class NamedFunction<T, R> implements Function<T, R> {
    private final @Nonnull String name;
    private final @Nonnull Function<T, R> wrapped;

    public NamedFunction(@Nonnull String name, @Nonnull Function<T, R> wrapped) {
        this.name = name;
        this.wrapped = wrapped;
    }

    public @Nonnull Function<T, R> getWrapped() {
        return wrapped;
    }

    @Override
    public R apply(T t) {
        return wrapped.apply(t);
    }

    @Override
    public String toString() {
        return name;
    }
}
