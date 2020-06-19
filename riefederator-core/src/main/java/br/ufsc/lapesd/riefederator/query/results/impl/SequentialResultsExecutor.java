package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import com.google.common.base.Stopwatch;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toSet;

public class SequentialResultsExecutor implements ResultsExecutor {
    private boolean closed = false;

    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> coll) {
        if (coll.size() == 1)
            return coll.iterator().next();
        Set<String> names = coll.stream().flatMap(r -> r.getVarNames().stream()).collect(toSet());
        return new SequentialResults(ResultsList.of(coll), names);
    }

    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> collection, int ignored) {
        return async(collection);
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
