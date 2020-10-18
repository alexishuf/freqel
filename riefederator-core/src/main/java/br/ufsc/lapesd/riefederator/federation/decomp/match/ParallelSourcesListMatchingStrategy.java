package br.ufsc.lapesd.riefederator.federation.decomp.match;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.query.CQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelSourcesListMatchingStrategy extends SourcesListMatchingStrategy {
    private static final Logger logger =
            LoggerFactory.getLogger(ParallelSourcesListMatchingStrategy.class);
    private ExecutorService pool = null;
    private AtomicReference<ArrayList<Future<Boolean>>> lastFutures = new AtomicReference<>();

    @Inject
    public ParallelSourcesListMatchingStrategy(@Nonnull PerformanceListener perfListener) {
        super(perfListener);
    }

    public ParallelSourcesListMatchingStrategy() { }

    private synchronized @Nonnull ExecutorService getPool() {
        if (pool == null) {
            int processors = Runtime.getRuntime().availableProcessors();
            int core = Math.min(sources.size(), processors);
            pool = new ThreadPoolExecutor(core, processors, 30, TimeUnit.SECONDS,
                    new LinkedBlockingDeque<>(),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }
        return pool;
    }

    @Override public @Nonnull Collection<Op> match(@Nonnull CQuery q,
                                                   @Nonnull Agglutinator agglutinator) {
        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(perfListener)) {
            return doMatch(q, agglutinator);
        }
    }

    private @Nonnull Collection<Op> doMatch(@Nonnull CQuery q, @Nonnull Agglutinator agglutinator) {
        ExecutorService pool = getPool();
        ArrayList<Future<Boolean>> fs = lastFutures.getAndSet(null);
        if (fs == null)
            fs = new ArrayList<>(sources.size());

        Agglutinator.State state = agglutinator.createState(q);
        for (Source s : sources)
            fs.add(pool.submit(() -> match(s, q, state)));
        try {
            int nMatches = 0;
            for (Future<Boolean> f : fs)  nMatches += f.get() ? 1 : 0;
            perfListener.sample(Metrics.SOURCES_COUNT, nMatches);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected interrupt on match/agglutinate", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) throw (RuntimeException)cause;
            else if (cause instanceof Error) throw (Error)cause;
            else throw new RuntimeException("Unexpected error on match/agglutinate", cause);
        } finally {
            fs.clear();
            lastFutures.set(fs);
        }
        return state.takeLeaves();
    }

    @Override public void close() {
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
                    logger.warn("Pending tasks in pool 1s after close()");
                    if (!pool.awaitTermination(10, TimeUnit.SECONDS))
                        logger.warn("Pending tasks in pool 11s after close(). Will stop waiting");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for pool termination");
            }
        }
        super.close();
    }
}
