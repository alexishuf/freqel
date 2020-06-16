package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

@NotThreadSafe
public class CollectionResults implements Results {
    private final @Nonnull Collection<? extends Solution> collection;
    private @Nullable String nodeName;
    private @LazyInit @Nullable Iterator<? extends Solution> iterator = null;
    private @LazyInit int size = -1;
    private @Nonnull final Set<String> varNames;

    public CollectionResults(@Nonnull Collection<? extends Solution> collection,
                             @Nonnull Collection<String> varNames) {
        this.collection = collection;
        this.varNames = varNames instanceof Set ? (Set<String>)varNames
                                                : ImmutableSet.copyOf(varNames);
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
    public @Nullable String getNodeName() {
        return nodeName;
    }

    @Override
    public void setNodeName(@Nullable String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public int getReadyCount() {
        return size == -1 ? collection.size() : size;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
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
