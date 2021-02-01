package br.ufsc.lapesd.freqel.query.annotations;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class GlobalContextAnnotation implements QueryAnnotation {
    public static final @Nonnull String USER_QUERY = "USER_QUERY";

    private final @Nonnull Map<Object, Object> map = new HashMap<>();

    public @Nullable Object get(@Nonnull Object k) {
        return map.get(k);
    }
    public @Nullable <T> T get(@Nonnull Object k, @Nonnull Class<T> expectedClass) {
        //noinspection unchecked
        return (T)map.get(k);
    }

    public synchronized @Nonnull <K, V>
    V computeIfAbsent(@Nonnull Object k, @Nonnull Function<? super K, ? extends V> function) {
        //noinspection unchecked
        return (V)map.computeIfAbsent(k, (Function<Object, Object>) function);
    }

    public synchronized @Nullable Object put(@Nonnull Object k, @Nullable Object value) {
        return map.put(k, value);
    }

    @Override public String toString() {
        return map.toString();
    }

    @Override public boolean equals(Object o) {
        return o instanceof GlobalContextAnnotation;
    }

    @Override public int hashCode() {
        return 0;
    }
}
