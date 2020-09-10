package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.AbstractResults;
import br.ufsc.lapesd.riefederator.query.results.BufferedResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

@NotThreadSafe
public class CollectionResults extends AbstractResults implements BufferedResults {
    private final @Nonnull Collection<? extends Solution> collection;
    private @LazyInit @Nullable Iterator<? extends Solution> iterator = null;
    private @LazyInit int size = -1;

    public CollectionResults(@Nonnull Collection<? extends Solution> collection,
                             @Nonnull Collection<String> varNames) {
        super(varNames);
        this.collection = collection;
    }

    public static  @Nonnull CollectionResults wrapSameVars(Collection<Solution> collection) {
        if (collection.isEmpty())
            return new CollectionResults(emptyList(), emptySet());
        return new CollectionResults(collection, collection.iterator().next().getVarNames());
    }

    public static @Nonnull CollectionResults empty() {
        return empty(emptySet());
    }

    public static @Nonnull CollectionResults empty(@Nonnull Collection<String> varNames) {
        return new CollectionResults(emptyList(), varNames);
    }

    public static @Nonnull CollectionResults greedy(@Nonnull Results other) {
        Set<String> vars = other.getVarNames();
        List<Solution> list = new ArrayList<>();
        other.forEachRemainingThenClose(list::add);
        return new CollectionResults(list, vars);
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public boolean isDistinct() {
        return collection instanceof Set;
    }

    @Override
    public void reset(boolean close) {
        /* nothing to close */
        iterator = null;
    }

    @Override
    public int getReadyCount() {
        return size == -1 ? collection.size() : size;
    }

    public @Nonnull Collection<? extends Solution> getCollection() {
        return collection;
    }

    private @Nonnull Iterator<? extends Solution> getIterator() {
        Iterator<? extends Solution> local = this.iterator;
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
