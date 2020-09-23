package br.ufsc.lapesd.riefederator.util.indexed;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Immutable
public class ImmFullIndexSet<T> extends FullIndexSet<T> implements ImmIndexSet<T> {
    private static final @Nonnull IndexSet<?> EMPTY
            = new FullIndexSet<>(ImmutableMap.of(), Collections.emptyList());
    private @LazyInit int hash = 0;

    protected ImmFullIndexSet(@Nonnull Map<T, Integer> indexMap, @Nonnull List<T> data) {
        super(indexMap, data);
    }

    @CheckReturnValue
    public static @Nonnull <U> FullIndexSet<U> empty() {
        //noinspection unchecked
        return (FullIndexSet<U>)EMPTY;
    }

    @Override public @Nonnull ImmIndexSet<T> asImmutable() {
        return this;
    }

    @Override public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override public int hashCode() {
        assert hash == 0 || hash == super.hashCode() : "Cached hash code became invalid!";
        return hash == 0 ? (hash = super.hashCode()) : hash;
    }
}
