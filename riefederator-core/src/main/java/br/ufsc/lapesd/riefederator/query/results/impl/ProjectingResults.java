package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public class ProjectingResults implements Results {
    private final @Nonnull Results delegate;
    private final @Nonnull Set<String> varNames;
    private @Nullable String nodeName;
    private final @Nonnull ArraySolution.ValueFactory factory;

    public ProjectingResults(@Nonnull Results delegate, @Nonnull Set<String> varNames) {
        this.delegate = delegate;
        this.varNames = varNames;
        this.factory = ArraySolution.forVars(varNames);
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        Projection projection = ModifierUtils.getFirst(Projection.class, query.getModifiers());
        if (projection != null)
            return new ProjectingResults(in, projection.getVarNames());
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
    public boolean isAsync() {
        return delegate.isAsync();
    }

    @Override
    public int getReadyCount() {
        return delegate.getReadyCount();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        return factory.fromFunction(delegate.next()::get);
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        delegate.close();
    }
}
