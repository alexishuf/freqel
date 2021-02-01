package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class IteratorResults extends AbstractResults implements Results {
    private final @Nonnull Iterator<? extends Solution> iterator;
    private @Nullable Solution solution;

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
        return solution != null ? 1 : 0;
    }

    @Override
    public boolean hasNext() {
        if (solution != null)
            return true;
        if (iterator.hasNext())
            solution = iterator.next();
        return solution != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        assert this.solution != null;
        Solution ret = this.solution;
        this.solution = null;
        return ret;
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
