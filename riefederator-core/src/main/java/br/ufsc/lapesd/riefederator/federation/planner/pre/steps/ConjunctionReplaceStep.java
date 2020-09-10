package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.federation.planner.utils.StepUtils;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ListIterator;
import java.util.Set;

public class ConjunctionReplaceStep implements PlannerStep {
    private static final Logger logger = LoggerFactory.getLogger(ConjunctionReplaceStep.class);

    private final @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public ConjunctionReplaceStep(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        if (!(root instanceof InnerOp))
            return root;
        InnerOp io = (InnerOp) root;
        try (TakenChildren children = io.takeChildren().setNoContentChange()) {
            for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                it.set(plan(it.next(), locked));
        }
        if (root instanceof ConjunctionOp) {
            if (locked.contains(RefEquals.of(root))) {
                assert false;
                logger.error("Locked ConjunctionOp {} MUST be replaced to be have an " +
                             "executable plan! Will ignore its locked status", root);
            }
            return visit((ConjunctionOp) root);
        }
        return root;
    }

    @Override public @Nonnull String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), joinOrderPlanner);
    }

    /* --- --- --- Internals --- --- --- */

    private @Nonnull Op visit(@Nonnull ConjunctionOp parent) {
        Op op = StepUtils.planConjunction(parent.getChildren(), joinOrderPlanner);
        new FilterAssigner(parent.modifiers().filters()).placeBottommost(op);
        TreeUtils.copyNonFilter(op, parent.modifiers());
        return op;
    }
}
