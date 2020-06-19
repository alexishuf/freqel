package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.AbstractResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

public class FlatMapResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(FlatMapResults.class);

    private final @Nonnull Results in;
    private final @Nonnull Function<Solution, Results> op;
    private @Nullable Results currentResults;

    public FlatMapResults(@Nonnull Results in, @Nonnull Set<String> varNames,
                          @Nonnull Function<Solution, Results> op) {
        super(varNames);
        this.in = in;
        this.op = op;
    }

    @Override
    public boolean isAsync() {
        return in.isAsync();
    }

    @Override
    public int getReadyCount() {
        return in.getReadyCount() +
                (currentResults == null ? 0 : currentResults.getReadyCount());
    }

    @Override
    public boolean hasNext() {
        while (currentResults == null || !currentResults.hasNext()) {
            if (!in.hasNext()) {
                if (currentResults != null) {
                    try {
                        currentResults.close();
                        currentResults = null;
                    } catch (ResultsCloseException e) {
                        logger.error("Problem closing flatmap currentResults", e);
                    }
                }
                return false;
            }
            currentResults = op.apply(in.next());
        }
        return true;
    }

    @Override
    public @Nonnull Solution next() {
        if (currentResults == null)
            throw new NoSuchElementException();
        assert currentResults.hasNext();
        Solution next = currentResults.next();
        assert getVarNames().equals(new HashSet<>(next.getVarNames()));
        return next;
    }

    @Override
    public void close() throws ResultsCloseException {
        ResultsCloseException exception = null;
        if (currentResults != null) {
            try {
                currentResults.close();
            } catch (ResultsCloseException e) {
                exception = e;
            }
        }
        try {
            in.close();
        } catch (ResultsCloseException e) {
            if (exception == null) exception  = e;
            else                   exception.addSuppressed(e);
        }
        if (exception != null)
            throw exception;
    }
}
