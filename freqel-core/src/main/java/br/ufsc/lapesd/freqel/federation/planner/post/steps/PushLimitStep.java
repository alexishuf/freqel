package br.ufsc.lapesd.freqel.federation.planner.post.steps;

import br.ufsc.lapesd.freqel.algebra.InnerOp;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.TakenChildren;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.freqel.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.freqel.query.modifiers.Limit;
import br.ufsc.lapesd.freqel.util.ref.RefSet;

import javax.annotation.Nonnull;

public class PushLimitStep implements PlannerStep, PlannerShallowStep {
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

    @Override public @Nonnull Op visit(@Nonnull Op op, @Nonnull RefSet<Op> shared) {
        Limit limit = op.modifiers().limit();
        if (limit == null)
            return op;
        if (!(op instanceof UnionOp) && !(op instanceof CartesianOp))
            return op;
        if (!op.modifiers().filters().isEmpty())
            return op;
        try (TakenChildren children = ((InnerOp) op).takeChildren().setNoContentChange()) {
            for (int i = 0, size = children.size(); i < size; i++) {
                Op child = children.get(i);
                if (shared.contains(child))
                    child = new PipeOp(child);
                child.modifiers().add(limit);
                children.set(i, child);
            }
        }
        return op;
    }
}
