package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.EXACT;

@NotThreadSafe
public class CollectionResults implements Results {
    private final @Nonnull Collection<Solution> collection;
    private @LazyInit @Nullable Iterator<Solution> iterator = null;
    private @LazyInit int size = -1;
    private @Nonnull Set<String> varNames;

    public CollectionResults(@Nonnull Collection<Solution> collection,
                             @Nonnull Set<String> varNames) {
        this.collection = collection;
        this.varNames = varNames;
    }

    @Override
    public int getReadyCount() {
        return size == -1 ? collection.size() : size;
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        if (size == -1)
            return new Cardinality(EXACT, collection.size());
        return new Cardinality(EXACT, size);
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    private @Nonnull Iterator<Solution> getIterator() {
        Iterator<Solution> local = this.iterator;
        if (local == null) {
            iterator = local = collection.iterator();
            size = collection.size();
        }
        return local;
    }

    @Override
    public boolean hasNext() {
        return getIterator().hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        Solution solution = getIterator().next();
        --size;
        return solution;
    }

    @Override
    public void close() { }
}
