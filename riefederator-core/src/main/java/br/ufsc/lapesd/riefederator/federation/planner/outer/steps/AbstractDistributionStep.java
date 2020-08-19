package br.ufsc.lapesd.riefederator.federation.planner.outer.steps;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.planner.outer.OuterPlannerShallowStep;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.util.RefEquals;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public abstract class AbstractDistributionStep implements OuterPlannerShallowStep {
    protected  @Nullable QueryOp getQuery(@Nonnull Op parent,
                                          @Nonnull Set<RefEquals<Op>> locked) {
        assert parent.getChildren().stream().filter(c -> c instanceof QueryOp
                && !c.modifiers().contains(Optional.INSTANCE)
                && !locked.contains(RefEquals.of(c))).count() <= 1;
        for (Op child : parent.getChildren()) {
            if (child instanceof QueryOp && !child.modifiers().contains(Optional.INSTANCE)
                    && !locked.contains(RefEquals.of(child))) {
                return (QueryOp) child;
            }
        }
        return null;
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }
}
