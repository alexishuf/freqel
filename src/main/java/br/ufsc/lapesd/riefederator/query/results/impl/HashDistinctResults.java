package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class HashDistinctResults implements Results {
    private final @Nonnull HashSet<Solution> set = new HashSet<>();
    private final @Nonnull Results input;
    private @Nullable String nodeName;
    private Solution next = null;

    public HashDistinctResults(@Nonnull Results input) {
        this.input = input;
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        if (ModifierUtils.getFirst(Capability.DISTINCT, query.getModifiers()) != null)
            return new HashDistinctResults(in);
        return in;
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
    public int getReadyCount() {
        return input.getReadyCount() + (next != null ? 1 : 0);
    }

    @Override
    public boolean hasNext() {
        while (this.next == null && input.hasNext()) {
            Solution next = input.next();
            if (set.add(next))
                this.next = next;
        }
        return this.next != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution current = this.next;
        this.next = null;
        return current;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return input.getVarNames();
    }

    @Override
    public void close() throws ResultsCloseException {
        input.close();
    }
}
