package br.ufsc.lapesd.freqel.query.results;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    /**
     * Indicates whether this Results object is accelerated by background processing.
     *
     * The hasNext()/next() remain not thread safe and must be used by a single thread.
     */
    boolean isAsync();

    /**
     * Indicates whether this object never returns duplicate {@link Solution}s
     */
    boolean isDistinct();

    /**
     * Indicates whether the existence of solutions in this results should be considered optional.
     *
     * When optional, the lack of solutions does not cause result elimination: A join with
     * optional arguments becomes a (left/right) outer join and a cartesian product returns one
     * of the sides instead of zero solutions if one of the sides is optional and has no solutions.
     */
    boolean isOptional();

    /**
     * If this results has an upper bound in the number of results return that bound, else -1.
     */
    int getLimit();

    /**
     * Set the optional flag reported by {@link Results#isOptional()}.
     */
    void setOptional(boolean value);

    /**
     * Wait for availability of a next() element for at most the given ammount of milliseconds.
     *
     * If the timeout expires, returns false. <b>Note that this does not means the Results
     * is empty or that a future hasNext() call wouldn't return true.</b>
     *
     * Implementations are not required to implement the timeout. This should only be
     * implemented by asynchronous implementations ({@link Results#isAsync()}) if there is
     * an efficient way of implementing it. If the implementation does no honor the timeout,
     * it should block indefinitely, as {@link Iterator#hasNext()} does.
     *
     * @param millisecondsTimeout timeout in milliseconds
     * @return true if there is a value, false if exhausted or timed out.
     */
    default boolean hasNext(int millisecondsTimeout) {
        return hasNext();
    }

    @Override
    @Nonnull Solution next();

    /**
     * Set of variable names (the x in ?x) that {@link Solution}s from next() may contain.
     *
     * @return unmodifiable {@link Set} with var names
     */
    @Nonnull Set<String> getVarNames();

    /**
     * If this Results represents a plan node, this is the name of such node.
     *
     * Used for logging & debug.
     */
    @Nullable String getNodeName();

    void setNodeName(@Nonnull String name);

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
