package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;

import javax.annotation.Nonnull;
import java.util.Set;

public abstract class JoinResults implements Results {
    private final @Nonnull Set<String> varNames;
    private final @Nonnull ResultsList children;

    public JoinResults(@Nonnull Set<String> varNames,
                       @Nonnull ResultsList children) {
        this.varNames = varNames;
        this.children = children;
    }

    @Override
    public int getReadyCount() {
        return hasNext() ? 1 : 0;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        children.close();
    }
}
