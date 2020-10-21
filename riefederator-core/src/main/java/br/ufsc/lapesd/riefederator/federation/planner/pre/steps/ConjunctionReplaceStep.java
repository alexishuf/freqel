package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.federation.planner.utils.StepUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.UnsafeMergeException;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.emptySet;

public class ConjunctionReplaceStep implements PlannerStep, PlannerShallowStep {
    private static final Logger logger = LoggerFactory.getLogger(ConjunctionReplaceStep.class);

    private final @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public ConjunctionReplaceStep(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
        if (!(root instanceof InnerOp))
            return root;
        InnerOp io = (InnerOp) root;
        boolean cartesianParent = io instanceof CartesianOp;
        Set<String> parentVars = io.getResultVars();
        boolean parentProjected = io.modifiers().projection() != null;
        try (TakenChildren children = io.takeChildren().setNoContentChange()) {
            for (int i = 0, childrenSize = children.size(); i < childrenSize; i++) {
                Op c = children.get(i);
                Op replacement = plan(c, shared);
                if (cartesianParent && replacement instanceof CartesianOp) {
                    try { //merge children of replacement into io
                        Set<String> fallback = parentProjected ? emptySet() : c.getResultVars();
                        io.modifiers().safeMergeWith(c.modifiers(), parentVars, fallback);
                        Iterator<Op> it = ((CartesianOp) replacement).takeChildren().iterator();
                        if (it.hasNext()) {
                            children.set(i, it.next());
                            while (it.hasNext()) {
                                children.add(++i, it.next());
                                ++childrenSize;
                            }
                        }
                    } catch (UnsafeMergeException e) {
                        children.set(i, replacement); // cannot merge, add the CartesianOp
                    }
                } else {
                    children.set(i, replacement); // just replace he node
                }
            }
        }
        if (root instanceof ConjunctionOp) {
            if (shared.contains(root)) {
                assert false;
                logger.error("Locked ConjunctionOp {} MUST be replaced to be have an " +
                             "executable plan! Will ignore its locked status", root);
            }
            return visit((ConjunctionOp) root);
        }
        return root;
    }

    @Override public @Nonnull Op visit(@Nonnull Op op, @Nonnull RefSet<Op> shared) {
        if (op instanceof ConjunctionOp) {
            if (shared.contains(op)) {
                assert false;
                logger.error("Locked ConjunctionOp {} MUST be replaced to be have an " +
                        "executable plan! Will ignore its locked status", op);
            }
            return visit((ConjunctionOp) op);
        }
        return op;
    }

    @Override public @Nonnull String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), joinOrderPlanner);
    }

    /* --- --- --- Internals --- --- --- */

    private @Nonnull Op visit(@Nonnull ConjunctionOp p) {
        Op op = StepUtils.planConjunction(p.getChildren(), p.modifiers(), joinOrderPlanner);
        FilterAssigner.placeInnerBottommost(op, p.modifiers().filters());
        return op;
    }
}
