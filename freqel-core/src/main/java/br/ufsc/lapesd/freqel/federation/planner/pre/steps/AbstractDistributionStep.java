package br.ufsc.lapesd.freqel.federation.planner.pre.steps;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.freqel.util.ref.RefSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AbstractDistributionStep implements PlannerShallowStep {
    protected  @Nullable QueryOp getQuery(@Nonnull Op parent,
                                          @Nonnull RefSet<Op> locked) {
        assert parent.getChildren().stream().filter(c -> c instanceof QueryOp
                && c.modifiers().optional() == null
                && !locked.contains(c)).count() <= 1;
        for (Op child : parent.getChildren()) {
            if (child instanceof QueryOp && child.modifiers().optional() == null
                    && !locked.contains(child)) {
                return (QueryOp) child;
            }
        }
        return null;
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }
}
