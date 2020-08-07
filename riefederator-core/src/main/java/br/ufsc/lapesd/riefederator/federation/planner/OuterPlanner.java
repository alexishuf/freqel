package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.UnassignedQueryOp;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

/**
 * An {@link OuterPlanner} is a planner that is intended to run before
 * decomposition (and source selection). Its goal is to break up a query into a {@link Op}
 * tree whose leaves are {@link UnassignedQueryOp} instances each with a conjunctive query.
 */
public interface OuterPlanner {
    /**
     * Break the query into a tree whose with {@link UnassignedQueryOp}s conjunctive queries as leaves.
     *
     * If the query is already conjunctive, will return a {@link UnassignedQueryOp}
     *
     * @param query a query
     * @return A plan whose leaves are {@link UnassignedQueryOp}s with conjunctive queries
     */
    @Nonnull Op plan(@Nonnull CQuery query);
}
