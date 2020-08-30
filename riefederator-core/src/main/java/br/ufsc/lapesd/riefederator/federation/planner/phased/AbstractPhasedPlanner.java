package br.ufsc.lapesd.riefederator.federation.planner.phased;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static java.lang.String.format;

public class AbstractPhasedPlanner {
    protected final @Nonnull List<PlannerStep> phase1deep = new ArrayList<>(),
                                               phase3deep = new ArrayList<>();
    protected final @Nonnull List<PlannerShallowStep> phase2shallow = new ArrayList<>();


    public @Nonnull AbstractPhasedPlanner appendPhase1(@Nonnull PlannerStep step) {
        phase1deep.add(step);
        return this;
    }

    public @Nonnull AbstractPhasedPlanner appendPhase2(@Nonnull PlannerShallowStep step) {
        phase2shallow.add(step);
        return this;
    }

    public @Nonnull AbstractPhasedPlanner appendPhase3(@Nonnull PlannerStep step) {
        phase3deep.add(step);
        return this;
    }

    @Override
    public String toString() {
        return format("%s{\n  %s,\n  %s,\n %s}",
                      getClass().getSimpleName(), phase1deep, phase2shallow, phase3deep);
    }

    public @Nonnull Op plan(@Nonnull Op tree) {
        Set<RefEquals<Op>> shared = TreeUtils.findSharedNodes(tree);
        for (PlannerStep step : phase1deep)
            tree = step.plan(tree, shared);
        tree = phase2(tree, shared);
        for (PlannerStep step : phase3deep)
            tree = step.plan(tree, shared);
        return tree;
    }

    protected  @Nonnull Op phase2(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        if (root instanceof InnerOp) { //recurse
            InnerOp io = (InnerOp) root;
            try (TakenChildren children = io.takeChildren().setNoContentChange()) {
                for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                    it.set(phase2(it.next(), locked));
            }
        }
        for (PlannerShallowStep step : phase2shallow) // do shallow visit
            root = step.visit(root, locked);
        return root;
    }
}
