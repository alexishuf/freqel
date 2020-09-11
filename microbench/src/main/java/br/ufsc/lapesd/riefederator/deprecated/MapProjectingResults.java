package br.ufsc.lapesd.riefederator.deprecated;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.ProjectingResults;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public class MapProjectingResults implements Results {
    private final @Nonnull Results delegate;
    private final @Nonnull Set<String> varNames;
    private @Nullable String nodeName;

    public MapProjectingResults(@Nonnull Results delegate, @Nonnull Set<String> varNames) {
        this.delegate = delegate;
        this.varNames = varNames;
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        Projection projection = query.getModifiers().projection();
        if (projection != null)
            return new ProjectingResults(in, projection.getVarNames());
        return in;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override public boolean isOptional() {
        return false;
    }

    @Override public int getLimit() {
        return -1;
    }

    @Override public void setOptional(boolean value) {
        throw new UnsupportedOperationException();
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

    @Nonnull
    @Override
    public Solution next() {
        Solution solution = delegate.next();
        MapSolution.Builder b = MapSolution.builder();
        for (String name : varNames) {
            Term term = solution.get(name, null);
            b.put(name, term);
        }
        return b.build();
    }

    @Nonnull
    @Override
    public Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        delegate.close();
    }
}
