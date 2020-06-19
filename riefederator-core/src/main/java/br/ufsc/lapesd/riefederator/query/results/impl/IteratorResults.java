package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.AbstractResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Set;

public class IteratorResults extends AbstractResults implements Results {
    private final @Nonnull Iterator<? extends Solution> iterator;

    public IteratorResults(@Nonnull Iterator<? extends Solution> iterator,
                           @Nonnull Set<String> varNames) {
        super(varNames);
        this.iterator = iterator;
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public int getReadyCount() {
        return iterator.hasNext() ? 1 : 0;
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
