package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.federation.tree.ComponentNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

/**
 * An {@link OuterPlanner} is a planner that is intended to run before
 * decomposition (and source selection). Its goal is to break up a query into a {@link PlanNode}
 * tree whose leaves are {@link ComponentNode} instances each with a conjunctive query.
 */
public interface OuterPlanner {
    /**
     * Break the query into a tree whose with {@link ComponentNode}s conjunctive queries as leaves.
     *
     * If the query is already conjunctive, will return a {@link ComponentNode}
     *
     * @param query a query
     * @return A plan whose leaves are {@link ComponentNode}s with conjunctive queries
     */
    @Nonnull PlanNode plan(@Nonnull CQuery query);
}
