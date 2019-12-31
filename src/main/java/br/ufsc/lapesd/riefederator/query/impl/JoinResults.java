package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.CardinalityComparator;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.ResultsList;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;

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
    public @Nonnull Cardinality getCardinality() {
        Cardinality min = children.get(0).getCardinality();
        CardinalityComparator comparator = new CardinalityComparator();
        for (Results child : children) {
            Cardinality candidate = child.getCardinality();
            if (comparator.compare(candidate, min) < 0)
                min = candidate;
        }
        return min;
    }

    @Override
    public void close() throws ResultsCloseException {
        children.close();
    }
}
