package br.ufsc.lapesd.riefederator.federation.planner.pre;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.PrePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.AbstractPhasedPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class PhasedPrePlanner extends AbstractPhasedPlanner implements PrePlanner {
    private final @Nonnull PerformanceListener performance;

    @Inject
    public PhasedPrePlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    public @Nonnull PhasedPrePlanner appendPhase1(@Nonnull PlannerStep step) {
        return (PhasedPrePlanner)super.appendPhase1(step);
    }

    public @Nonnull PhasedPrePlanner appendPhase2(@Nonnull PlannerShallowStep step) {
        return (PhasedPrePlanner)super.appendPhase2(step);
    }

    public @Nonnull PhasedPrePlanner appendPhase3(@Nonnull PlannerStep step) {
        return (PhasedPrePlanner)super.appendPhase3(step);
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op tree) {
        try (TimeSampler ignored = Metrics.PRE_PLAN_MS.createThreadSampler(performance)) {
            return super.plan(tree);
        }
    }
}
