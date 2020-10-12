package br.ufsc.lapesd.riefederator.federation.planner.pre;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.PrePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.AbstractPhasedPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.util.ref.EmptyRefSet;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;

public class PhasedPrePlanner extends AbstractPhasedPlanner implements PrePlanner {
    private final @Nonnull PerformanceListener performance;

    @Inject
    public PhasedPrePlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    @Override public @Nonnull PhasedPrePlanner addDeepPhase(@Nonnull List<PlannerStep> steps) {
        return (PhasedPrePlanner)super.addDeepPhase(steps);
    }

    @Override
    public @Nonnull PhasedPrePlanner addShallowPhase(@Nonnull List<PlannerShallowStep> steps) {
        return (PhasedPrePlanner)super.addShallowPhase(steps);
    }

    @Override protected @Nonnull RefSet<Op> getShared(@Nonnull Op tree) {
        return EmptyRefSet.emptySet();
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op tree) {
        try (TimeSampler ignored = Metrics.PRE_PLAN_MS.createThreadSampler(performance)) {
            return super.plan(tree);
        }
    }
}
