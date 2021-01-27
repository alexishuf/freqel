package br.ufsc.lapesd.freqel.federation.planner.post.steps;

import br.ufsc.lapesd.freqel.algebra.InnerOp;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.TakenChildren;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.freqel.query.modifiers.Distinct;
import br.ufsc.lapesd.freqel.util.ref.RefSet;

import javax.annotation.Nonnull;

public class PushDistinctStep implements PlannerStep {
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
        return visit(root, shared, false);
    }

    @Override
    public @Nonnull String toString() {
        return getClass().getSimpleName();
    }

    private static @Nonnull Op visit(@Nonnull Op op, @Nonnull RefSet<Op> shared,
                                     boolean makeDistinct) {
        boolean distinct = op.modifiers().distinct() != null;
        if (op instanceof InnerOp) {
            boolean effMakeDistinct = makeDistinct || distinct;
            try (TakenChildren children = ((InnerOp) op).takeChildren().setNoContentChange()) {
                for (int i = 0, size = children.size(); i < size; i++)
                    children.set(i, visit(children.get(i), shared, effMakeDistinct));
            }
        } else if (makeDistinct && !distinct) {
            if (shared.contains(op))
                op = new PipeOp(op);
            op.modifiers().add(Distinct.INSTANCE);
        }
        return op;
    }
}
