package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.*;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

class SequentialResults extends AbstractResults implements Results {
    private final @Nonnull ResultsList<? extends Results> results;
    private int idx = 0;

    public SequentialResults(@Nonnull Collection<? extends Results> results, @Nonnull Set<String> varNames) {
        super(varNames);
        this.results = ResultsList.of(results);
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public int getReadyCount() {
        if (idx >= results.size()) return 0;
        return results.get(idx).getReadyCount();
    }

    @Override
    public boolean hasNext() {
        while (idx < results.size()) {
            if (results.get(idx).hasNext())
                return true;
            else
                ++idx;
        }
        return false;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        return results.get(idx).next();
    }

    @Override
    public void close() throws ResultsCloseException {
        results.close();
    }
}
