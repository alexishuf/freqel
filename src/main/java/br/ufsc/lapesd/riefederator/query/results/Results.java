package br.ufsc.lapesd.riefederator.query.results;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

@NotThreadSafe
public interface Results extends Iterator<Solution>, AutoCloseable {
    /**
     * Number of solutions in-memory and ready for consumption with minimal delay.
     */
    int getReadyCount();

    @Override
    @Nonnull Solution next();

    /**
     * Set of variable names (the x in ?x) that {@link Solution}s from next() may contain.
     *
     * @return unmodifiable {@link Set} with var names
     */
    @Nonnull Set<String> getVarNames();

    /**
     * Equivalent to {@link #forEachRemaining(Consumer)} followed by a call to {@link #close()}
     *
     * @param action consumer of the {@link Solution}s
     * @throws ResultsCloseException may be thrown by a {@link #close()} implementation.
     */
    default void forEachRemainingThenClose(Consumer<? super Solution> action)
            throws ResultsCloseException {
        try {
            while (hasNext())
                action.accept(next());
        } finally {
            close();
        }
    }

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
