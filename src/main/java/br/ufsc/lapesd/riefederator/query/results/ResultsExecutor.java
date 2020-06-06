package br.ufsc.lapesd.riefederator.query.results;

import javax.annotation.Nonnull;
import java.util.Collection;

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
     * @return A meta {@link Results} object for consuming the non-deduplicated results
     *         from the input objects
     */
    @Nonnull Results async(@Nonnull Collection<? extends Results> collection);

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
}
