package br.ufsc.lapesd.riefederator.federation.decomp.match;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.concurrent.CommonPoolPlanningExecutorService;
import br.ufsc.lapesd.riefederator.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.riefederator.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelSourcesListMatchingStrategy extends SourcesListMatchingStrategy {
    private final @Nonnull PlanningExecutorService executor;
    private AtomicReference<ArrayList<Future<Boolean>>> lastFutures = new AtomicReference<>();

    @Inject
    public ParallelSourcesListMatchingStrategy(@Nonnull PlanningExecutorService executorService,
                                               @Nonnull PerformanceListener perfListener) {
        super(perfListener);
        this.executor = executorService;
    }

    public ParallelSourcesListMatchingStrategy() {
        executor = CommonPoolPlanningExecutorService.INSTANCE;
    }

    @Override public @Nonnull Collection<Op> match(@Nonnull CQuery q,
                                                   @Nonnull Agglutinator agglutinator) {
        AtomicInteger nMatches = new AtomicInteger(0);
        try (TimeSampler ignored = Metrics.SELECTION_MS.createThreadSampler(perfListener)) {
            Agglutinator.State state = agglutinator.createState(q);
            executor.parallelFor(0, sources.size(), i -> {
                if (match(sources.get(i), q, state))
                    nMatches.incrementAndGet();
            });
            perfListener.sample(Metrics.SOURCES_COUNT, nMatches.get());
            return state.takeLeaves();
        }
    }
}
