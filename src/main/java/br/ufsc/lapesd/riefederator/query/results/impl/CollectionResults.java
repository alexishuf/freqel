package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.Collections;
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
                             @Nonnull Collection<String> varNames) {
        this.collection = collection;
        this.varNames = varNames instanceof Set ? (Set<String>)varNames
                                                : ImmutableSet.copyOf(varNames);
    }

    public static @Nonnull CollectionResults empty(@Nonnull Collection<String> varNames) {
        return new CollectionResults(Collections.emptyList(), varNames);
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

    public @Nonnull Collection<Solution> getCollection() {
        return collection;
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
