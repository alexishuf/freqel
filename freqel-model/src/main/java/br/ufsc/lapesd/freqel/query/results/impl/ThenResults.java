package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

public class ThenResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(ThenResults.class);
    private final @Nonnull Iterator<? extends Callable<Results>> factoryIt;
    private @Nullable Results active = null;

    public ThenResults(@Nonnull Collection<String> varNames,
                       @Nonnull Iterator<? extends Callable<Results>> factoryIt) {
        super(varNames);
        this.factoryIt = factoryIt;
    }

    public ThenResults(@Nonnull Collection<String> varNames,
                       @Nonnull Collection<? extends Callable<Results>> factories) {
        this(varNames, factories.iterator());
    }

    @Override public int getReadyCount() {
        return active == null ? 0 : active.getReadyCount();
    }

    @Override public boolean hasNext() {
        while (active == null || !active.hasNext()) {
            if (active != null)
                closeActive();
            if (!factoryIt.hasNext())
                return false;
            Callable<Results> callable = factoryIt.next();
            try {
                active = callable.call();
            } catch (Throwable t) {
                logger.error("Failed to create next Results from {}", callable, t);
                assert false; // only blow up in debug builds
            }
            assert active != null;
            closeCallable(callable);
        }
        return true;
    }

    private void closeCallable(Callable<Results> callable) {
        if (callable instanceof AutoCloseable) {
            try {
                ((AutoCloseable) callable).close();
            } catch (Throwable t) {
                logger.error("Failed to close() {}", callable, t);
                assert false; // only blow up in debug builds
            }
        }
    }

    private void closeActive() {
        if (active != null) {
            try {
                active.close();
            } catch (Throwable t) {
                logger.error("{}.close failed", active, t);
                assert false; // blow up, but only in debug
            }
            active = null;
        }
    }

    @Override public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException();
        assert active != null;
        return active.next();
    }

    @Override public void close() throws ResultsCloseException {
        closeActive();
        while (factoryIt.hasNext())
            closeCallable(factoryIt.next());
    }
}
