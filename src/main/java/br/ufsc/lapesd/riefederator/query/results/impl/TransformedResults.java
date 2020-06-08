package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class TransformedResults implements Results {
    private final @Nonnull Results in;
    private final @Nonnull Function<Solution, Solution> op;
    private final @Nonnull Set<String> varNames;
    private @Nullable String nodeName;

    public TransformedResults(@Nonnull Results in, @Nonnull Set<String> varNames,
                              @Nonnull Function<Solution, Solution> op) {
        this.in = in;
        this.op = op;
        this.varNames = varNames;
    }

    @Override
    public @Nullable String getNodeName() {
        return nodeName != null ? nodeName : in.getNodeName();
    }

    @Override
    public void setNodeName(@Nonnull String name) {
        this.nodeName = name;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public boolean isAsync() {
        return in.isAsync();
    }

    @Override
    public int getReadyCount() {
        return in.getReadyCount();
    }

    @Override
    public boolean hasNext() {
        return in.hasNext();
    }


    @Override
    public @Nonnull Solution next() {
        Solution solution = op.apply(in.next());
        assert getVarNames().equals(new HashSet<>(solution.getVarNames()));
        return solution;
    }

    @Override
    public void close() throws ResultsCloseException {
        in.close();
    }
}
