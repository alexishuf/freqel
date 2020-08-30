package br.ufsc.lapesd.riefederator.federation.planner.pre;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.PrePlanner;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static java.lang.String.format;

public class PhasedPrePlanner implements PrePlanner {
    private final @Nonnull PerformanceListener performance;
    private final @Nonnull List<PrePlannerStep> phase1deep = new ArrayList<>(),
                                                  phase3deep = new ArrayList<>();
    private final @Nonnull List<PrePlannerShallowStep> phase2shallow = new ArrayList<>();

    @Inject
    public PhasedPrePlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    public @Nonnull PhasedPrePlanner appendPhase1(@Nonnull PrePlannerStep step) {
        phase1deep.add(step);
        return this;
    }

    public @Nonnull PhasedPrePlanner appendPhase2(@Nonnull PrePlannerShallowStep step) {
        phase2shallow.add(step);
        return this;
    }

    public @Nonnull PhasedPrePlanner appendPhase3(@Nonnull PrePlannerStep step) {
        phase3deep.add(step);
        return this;
    }

    @Override
    public String toString() {
        return format("PhasedOuterPlanner{%s,   %s,   %s}", phase1deep, phase2shallow, phase3deep);
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op tree) {
        try (TimeSampler ignored = Metrics.OUT_PLAN_MS.createThreadSampler(performance)) {
            Set<RefEquals<Op>> shared = TreeUtils.findSharedNodes(tree  );
            for (PrePlannerStep step : phase1deep)
                tree = step.plan(tree, shared);
            tree = phase2(tree, shared);
            for (PrePlannerStep step : phase3deep)
                tree = step.plan(tree, shared);
            return tree;
        }
    }

    private @Nonnull Op phase2(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        if (root instanceof InnerOp) { //recurse
            InnerOp io = (InnerOp) root;
            try (TakenChildren children = io.takeChildren().setNoContentChange()) {
                for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                    it.set(phase2(it.next(), locked));
            }
        }
        for (PrePlannerShallowStep step : phase2shallow) // do shallow visit
            root = step.visit(root, locked);
        return root;
    }
}
