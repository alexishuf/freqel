package br.ufsc.lapesd.freqel.util.indexed.ref;

import br.ufsc.lapesd.freqel.util.indexed.ImmIndexSet;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.IdentityHashMap;
import java.util.List;

public class ImmRefIndexSet<T> extends RefIndexSet<T> implements ImmIndexSet<T> {
    private @LazyInit int hash = 0;

    protected ImmRefIndexSet(@Nonnull IdentityHashMap<T, Integer> indexMap, @Nonnull List<T> data) {
        super(indexMap, data);
    }

    @Override public @Nonnull ImmRefIndexSet<T> asImmutable() {
        return this;
    }

    @Override public int hashCode() {
        assert hash == 0 || hash == super.hashCode() : "Cached hash became invalid";
        return hash == 0 ? (hash = super.hashCode()) : hash;
    }
}
