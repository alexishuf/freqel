package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.Set;

@NotThreadSafe
public interface Results extends Iterator<Solution>, AutoCloseable {
    /**
     * Number of solutions in-memory and ready for consumption with minimal delay.
     */
    int getReadyCount();

    @Override
    @Nonnull Solution next();

    /**
     * Get the cardinality of this iterator.
     */
    @Nonnull Cardinality getCardinality();

    /**
     * Set of variable names (the x in ?x) that {@link Solution}s from next() may contain.
     *
     * @return unmodifiable {@link Set} with var names
     */
    @Nonnull Set<String> getVarNames();


    /**
     * Closes the {@link Results} object. This may be a no-op
     *
     * @throws ResultsCloseException {@link RuntimeException} for wrapping any exception
     *                               from close()ing inner components. Usually a {@link Results}
     *                               will not throw anything.
     */
    @Override
    void close() throws ResultsCloseException;
}
