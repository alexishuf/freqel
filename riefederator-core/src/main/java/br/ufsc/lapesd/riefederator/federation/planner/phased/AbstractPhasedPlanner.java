package br.ufsc.lapesd.riefederator.federation.planner.phased;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public abstract class AbstractPhasedPlanner {
    protected final @Nonnull List<Phase> phases = new ArrayList<>();

    protected interface Phase {
        @Nonnull Op run(@Nonnull Op root, @Nonnull RefSet<Op> shared);
    }

    protected static class ShallowPhase implements Phase {
        private List<PlannerShallowStep> steps;

        public ShallowPhase(List<PlannerShallowStep> steps) {
            this.steps = steps;
        }

        @Override public @Nonnull Op run(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
            assert root.assertTreeInvariants();
            if (root instanceof InnerOp) { //recurse
                InnerOp io = (InnerOp) root;
                try (TakenChildren children = io.takeChildren().setNoContentChange()) {
                    for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                        it.set(run(it.next(), shared));
                }
            }
            assert root.assertTreeInvariants();
            for (PlannerShallowStep step : steps) {// do shallow visit
                root = step.visit(root, shared);
                assert root.assertTreeInvariants();
            }
            return root;
        }
    }

    protected static class DeepPhase implements Phase {
        private List<PlannerStep> steps;

        public DeepPhase(@Nonnull List<PlannerStep> steps) {
            this.steps = steps;
        }

        @Override public @Nonnull Op run(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
            assert root.assertTreeInvariants();
            for (PlannerStep step : steps) {
                root = step.plan(root, shared);
                assert root.assertTreeInvariants();
            }
            return root;
        }
    }

    public @Nonnull AbstractPhasedPlanner addDeepPhase(@Nonnull List<PlannerStep> steps) {
        phases.add(new DeepPhase(steps));
        return this;
    }
    public @Nonnull AbstractPhasedPlanner addShallowPhase(@Nonnull List<PlannerShallowStep> steps) {
        phases.add(new ShallowPhase(steps));
        return this;
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder(getClass().getSimpleName()).append("{\n");
        for (Phase phase : phases) {
            b.append("  ").append(phase).append('\n');
        }
        b.setLength(b.length()-1);
        return b.append('}').toString();
    }

    public @Nonnull Op plan(@Nonnull Op tree) {
        assert tree.assertTreeInvariants();
        RefSet<Op> shared = getShared(tree);
        for (Phase phase : phases) {
            tree = phase.run(tree, shared);
            assert tree.assertTreeInvariants();
        }
        return tree;
    }

    protected abstract @Nonnull RefSet<Op> getShared(@Nonnull Op tree);
}
