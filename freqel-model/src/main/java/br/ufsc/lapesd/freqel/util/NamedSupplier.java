package br.ufsc.lapesd.freqel.util;

import javax.annotation.Nonnull;
import javax.inject.Provider;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

public class NamedSupplier<T> implements Supplier<T>, Provider<T> {
    private final @Nonnull String name;
    private final @Nonnull Supplier<T> wrapped;

    public NamedSupplier(@Nonnull String name, @Nonnull Supplier<T> wrapped) {
        this.name = name;
        this.wrapped = wrapped;
    }

    public NamedSupplier(@Nonnull Class<? extends T> cls) {
        this.name = cls.getName().replaceAll("(\\w)\\w+\\.", "$1.");
        try {
            Constructor<? extends T> constructor = cls.getConstructor();
            wrapped = () -> {
                try {
                    return constructor.newInstance();
                } catch (InstantiationException|IllegalAccessException|
                         InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            };
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T get() {
        return wrapped.get();
    }

    public @Nonnull String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }
}
