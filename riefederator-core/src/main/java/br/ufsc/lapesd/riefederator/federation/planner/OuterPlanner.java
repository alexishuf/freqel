package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.FreeQueryOp;

import javax.annotation.Nonnull;

/**
 * An {@link OuterPlanner} is a planner that is intended to run before
 * decomposition (and source selection). Its goal is to break up a query into a {@link Op}
 * tree whose leaves are {@link FreeQueryOp} instances each with a conjunctive query.
 */
public interface OuterPlanner {
    /**
     * Mutate the given query tree to enlarge conjunctions where possible.
     *
     * The rationale is that large conjunctions, in the form of {@link FreeQueryOp} nodes
     * in the tree can be pushed to the sources given some conditions, and thus reduce the
     * amount of data fetched by the mediator.

     * @param query a query
     * @return A plan whose leaves are {@link FreeQueryOp}s with conjunctive queries
     */
    @Nonnull Op plan(@Nonnull Op query);
}
