package br.ufsc.lapesd.riefederator.federation.planner.post;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.PostPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.AbstractPhasedPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;

public class PhasedPostPlanner extends AbstractPhasedPlanner implements PostPlanner {
    private final @Nonnull PerformanceListener performance;

    @Inject
    public PhasedPostPlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    @Override public @Nonnull PhasedPostPlanner addDeepPhase(@Nonnull List<PlannerStep> steps) {
        return (PhasedPostPlanner) super.addDeepPhase(steps);
    }

    @Override
    public @Nonnull PhasedPostPlanner addShallowPhase(@Nonnull List<PlannerShallowStep> steps) {
        return (PhasedPostPlanner) super.addShallowPhase(steps);
    }

    @Override protected @Nonnull RefSet<Op> getShared(@Nonnull Op tree) {
        return TreeUtils.findSharedNodes(tree);
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op tree) {
        try (TimeSampler ignored = Metrics.POST_PLAN_MS.createThreadSampler(performance)) {
            return super.plan(tree);
        }
    }
}
