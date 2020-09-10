package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.federation.planner.utils.FilterJoinPlanner;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> shared) {
        return new Replacer(shared).visit(root);
    }

    @Override
    public @Nonnull String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), filterJoinPlanner);
    }

    private class Replacer {
        @Nonnull Set<RefEquals<Op>> shared;
        @Nullable Map<RefEquals<Op>, Op> converted;

        public Replacer(@Nonnull Set<RefEquals<Op>> shared) {
            this.shared = shared;
        }

        public @Nonnull Op visit(@Nonnull Op op) {
            if (op instanceof CartesianOp || op instanceof JoinOp) {
                if (converted != null) {
                    Op previous = converted.get(RefEquals.of(op));
                    if (previous != null) return previous;
                }
                Op replacement;
                if (op instanceof CartesianOp)
                    replacement = filterJoinPlanner.rewrite((CartesianOp) op, shared);
                else
                    replacement = filterJoinPlanner.rewrite((JoinOp) op, shared);
                assert validFilterPlacements(replacement);
                if (replacement != op) {
                    RefEquals<Op> ref = RefEquals.of(op);
                    if (shared.contains(ref)) {
                        shared.add(RefEquals.of(replacement));
                        if (converted == null)
                            converted = new HashMap<>();
                        converted.put(ref, replacement);
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
