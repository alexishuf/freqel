package br.ufsc.lapesd.riefederator.query.results;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public interface ResultsExecutor extends AutoCloseable {
    /**
     * Execute all results in collection and redirect their results to the returned Results object.
     *
     * Execution of the {@link Results} objects in collection MAY occur in parallel. Buffering
     * policies may apply.
     *
     * Closing the returned {@link Results} object will stop any background consumption of
     * the {@link Results} given as input and will also close them. The interruption of
     * processing and closing of the {@link Results} may not be complete by the return
     * of this method.
     *
     * @param collection Set of {@link Results} objects to consume in parallel
     * @param bufferSizeHint a hint of how many results should be kept in a buffer, per
     *                       input Results. Can be ignored
     * @return A meta {@link Results} object for consuming the non-deduplicated results
     *         from the input objects
     */
    @Nonnull Results async(@Nonnull Collection<? extends Results> collection,
                           int bufferSizeHint);

    /**
     * Same as {@link ResultsExecutor#async(Collection, int)}, but without buffer hint
     */
    @Nonnull Results async(@Nonnull Collection<? extends Results> collection);

    default @Nonnull Results async(@Nonnull Results results) {
        return async(Collections.singleton(results));
    }

    /**
     * Closes the async executor.
     *
     * Subsequent calls to async() will yield empty Results objects.
     * Calling {@link Results#next()} on a object preciously returned
     * by {@link ResultsExecutor#async(Collection)} may return buffered data,
     * but {@link Results#hasNext()} will eventually return false.
     */
    @Override
    void close();

    /**
     * Blocks until any background thread or task that started shutdown at close finishes.
     *
     * If close() has not yet been called, this should block nevertheless.
     *
     * @param timeout how much time to wait at maximum
     * @param unit unit of timeout
     * @return true iff threads and background tasks are finished.
     */
    @CanIgnoreReturnValue
    boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException;
}
