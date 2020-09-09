package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import java.util.Set;

public class PushDistinctStep implements PlannerStep {
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> shared) {
        return visit(root, shared, false);
    }

    private static @Nonnull Op visit(@Nonnull Op op, @Nonnull Set<RefEquals<Op>> shared,
                                     boolean makeDistinct) {
        boolean distinct = op.modifiers().distinct() != null;
        if (op instanceof InnerOp) {
            boolean effMakeDistinct = makeDistinct || distinct;
            try (TakenChildren children = ((InnerOp) op).takeChildren().setNoContentChange()) {
                for (int i = 0, size = children.size(); i < size; i++)
                    children.set(i, visit(children.get(i), shared, effMakeDistinct));
            }
        } else if (makeDistinct && !distinct) {
            if (shared.contains(RefEquals.of(op)))
                op = new PipeOp(op);
            op.modifiers().add(Distinct.INSTANCE);
        }
        return op;
    }
}
