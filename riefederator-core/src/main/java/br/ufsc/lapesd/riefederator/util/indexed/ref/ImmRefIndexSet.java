package br.ufsc.lapesd.riefederator.util.indexed.ref;

import br.ufsc.lapesd.riefederator.util.indexed.ImmIndexSet;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.IdentityHashMap;
import java.util.List;

public class ImmRefIndexSet<T> extends RefIndexSet<T> implements ImmIndexSet<T> {
    private @LazyInit int hash = 0;

    protected ImmRefIndexSet(@Nonnull List<T> data,
                             @Nonnull IdentityHashMap<T, Integer> indexMap) {
        super(indexMap, data);
    }

    @Override public int hashCode() {
        assert hash == 0 || hash == super.hashCode() : "Cached hash became invalid";
        return hash == 0 ? (hash = super.hashCode()) : hash;
    }
}
