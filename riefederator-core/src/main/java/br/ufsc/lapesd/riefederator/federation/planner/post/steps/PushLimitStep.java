package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.modifiers.Limit;
import br.ufsc.lapesd.riefederator.util.RefSet;

import javax.annotation.Nonnull;

public class PushLimitStep implements PlannerStep {
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
        Limit limit = root.modifiers().limit();
        if (limit == null)
            return root;
        if (!(root instanceof UnionOp) && !(root instanceof CartesianOp))
            return root;
        if (!root.modifiers().filters().isEmpty())
            return root;
        try (TakenChildren children = ((InnerOp) root).takeChildren().setNoContentChange()) {
            for (int i = 0, size = children.size(); i < size; i++) {
                Op child = children.get(i);
                if (shared.contains(child))
                    child = new PipeOp(child);
                child.modifiers().add(limit);
                children.set(i, child);
            }
        }
        return root;
    }
}
