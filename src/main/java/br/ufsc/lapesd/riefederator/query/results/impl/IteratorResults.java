package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Set;

public class IteratorResults implements Results {
    private final @Nonnull Iterator<? extends Solution> iterator;
    private final @Nonnull Set<String> varNames;

    public IteratorResults(@Nonnull Iterator<? extends Solution> iterator,
                           @Nonnull Set<String> varNames) {
        this.iterator = iterator;
        this.varNames = varNames;
    }

    @Override
    public int getReadyCount() {
        return iterator.hasNext() ? 1 : 0;
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return iterator.hasNext() ? Cardinality.NON_EMPTY : Cardinality.EMPTY;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public @Nonnull Solution next() {
        return iterator.next();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void close() throws ResultsCloseException {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e) {
                throw new ResultsCloseException(this, e);
            }
        }
    }
}
