package br.ufsc.lapesd.freqel.federation.decomp.match;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.concurrent.CommonPoolPlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.annotations.GlobalContextAnnotation;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelSourcesListMatchingStrategy extends SourcesListMatchingStrategy {
    private final @Nonnull PlanningExecutorService executor;
    private final AtomicReference<ArrayList<Future<Boolean>>> lastFutures = new AtomicReference<>();

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
            GlobalContextAnnotation gCtx = q.getQueryAnnotation(GlobalContextAnnotation.class);
            return stampGlobalContext(state.takeLeaves(), gCtx);
        }
    }
}
