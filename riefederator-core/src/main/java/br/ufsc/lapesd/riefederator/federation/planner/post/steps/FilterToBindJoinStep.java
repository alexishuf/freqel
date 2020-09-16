package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.federation.planner.utils.FilterJoinPlanner;
import br.ufsc.lapesd.riefederator.util.RefSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.IdentityHashMap;

public class FilterToBindJoinStep implements PlannerStep {
    private final @Nonnull FilterJoinPlanner filterJoinPlanner;

    @Inject
    public FilterToBindJoinStep(@Nonnull FilterJoinPlanner filterJoinPlanner) {
        this.filterJoinPlanner = filterJoinPlanner;
    }

    /**
     * Tries to push FILTER clauses applied to {@link JoinOp} and {@link CartesianOp} nodes
     * into some of the children, using those FILTERs as part of a bind join instead of
     * evaluating them at mediator after the join/product.
     *
     * @param root the input plan (or query)
     * @param shared set of nodes that are shared at multiple points of the tree
     * @return the new plan root (modifications may not require a new root)
     */
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
        return new Replacer(shared).visit(root);
    }

    @Override
    public @Nonnull String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), filterJoinPlanner);
    }

    private class Replacer {
        @Nonnull RefSet<Op> shared;
        @Nullable IdentityHashMap<Op, Op> converted;

        public Replacer(@Nonnull RefSet<Op> shared) {
            this.shared = shared;
        }

        public @Nonnull Op visit(@Nonnull Op op) {
            if (op instanceof CartesianOp || op instanceof JoinOp) {
                if (converted != null) {
                    Op previous = converted.get(op);
                    if (previous != null) return previous;
                }
                Op replacement;
                if (op instanceof CartesianOp)
                    replacement = filterJoinPlanner.rewrite((CartesianOp) op, shared);
                else
                    replacement = filterJoinPlanner.rewrite((JoinOp) op, shared);
                assert validFilterPlacements(replacement);
                if (replacement != op) {
                    if (shared.contains(op)) {
                        shared.add(replacement);
                        if (converted == null)
                            converted = new IdentityHashMap<>(shared.size());
                        converted.put(op, replacement);
                    }
                }
                return replacement;
            } else if (op instanceof InnerOp) {
                try (TakenChildren children = ((InnerOp) op).takeChildren().setNoContentChange()) {
                    for (int i = 0, size = children.size(); i < size; i++)
                        children.set(i, visit(children.get(i)));
                }
            }
            return op;
        }

        private boolean validFilterPlacements(@Nonnull Op root) {
            return TreeUtils.streamPreOrder(root).allMatch(n -> n.modifiers().filters().stream()
                    .allMatch(f -> n.getAllVars().containsAll(f.getVarTermNames())));
        }
    }


}
