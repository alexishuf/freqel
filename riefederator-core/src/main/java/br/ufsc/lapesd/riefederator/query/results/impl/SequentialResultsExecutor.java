package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import com.google.common.base.Stopwatch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class SequentialResultsExecutor implements ResultsExecutor {
    private boolean closed = false;

    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> coll,
                                  @Nullable Collection<String> namesHint) {
        if (coll.size() == 1)
            return coll.iterator().next();
        return new SequentialResults(ResultsList.of(coll), namesHint);
    }

    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> collection,
                                  @Nullable Collection<String> namesHint, int ignored) {
        return async(collection, namesHint);
    }

    @Override
    public @Nonnull Results async(@Nonnull Results results) {
        return results;
    }

    @Override
    public synchronized void close() {
        closed = true;
        notifyAll();
    }

    @Override
    public boolean awaitTermination(long timeout,
                                    @Nonnull TimeUnit unit) throws InterruptedException {
        long ms = TimeUnit.MILLISECONDS.convert(timeout, unit);
        for (Stopwatch sw = Stopwatch.createStarted(); !closed && ms > 0; sw.reset().start()) {
            wait(ms);
            ms = Math.max(0, ms - sw.elapsed(TimeUnit.MILLISECONDS));
        }
        return closed;
    }
}
