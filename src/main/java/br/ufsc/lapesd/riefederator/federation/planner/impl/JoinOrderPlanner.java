package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;

import javax.annotation.Nonnull;
import java.util.List;

public interface JoinOrderPlanner {
    /**
     * Plans the order of execution of the given joins.
     *
     * @param joins List of joins that need to be executed.
     * @return A tree whose root corresponds to execution of all joins
     */
    @Nonnull PlanNode plan(@Nonnull List<JoinInfo> joins);
}
