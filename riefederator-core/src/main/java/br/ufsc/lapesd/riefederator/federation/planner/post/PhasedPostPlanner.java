package br.ufsc.lapesd.riefederator.federation.planner.post;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.PostPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.AbstractPhasedPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class PhasedPostPlanner extends AbstractPhasedPlanner implements PostPlanner {
    private final @Nonnull PerformanceListener performance;

    @Inject
    public PhasedPostPlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    public @Nonnull PhasedPostPlanner appendPhase1(@Nonnull PlannerStep step) {
        return (PhasedPostPlanner)super.appendPhase1(step);
    }

    public @Nonnull PhasedPostPlanner appendPhase2(@Nonnull PlannerShallowStep step) {
        return (PhasedPostPlanner)super.appendPhase2(step);
    }

    public @Nonnull PhasedPostPlanner appendPhase3(@Nonnull PlannerStep step) {
        return (PhasedPostPlanner)super.appendPhase3(step);
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op tree) {
        try (TimeSampler ignored = Metrics.POST_PLAN_MS.createThreadSampler(performance)) {
            return super.plan(tree);
        }
    }
}
