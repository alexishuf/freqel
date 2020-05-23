package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface Planner {
    /**
     * If true, this Planner allows queries given to plan() to be join-disconnected.
     *
     * Join disconnected queries require a cartesian product.
     */
    boolean allowJoinDisconnected();

    /**
     * Builds a plan, as a tree, for the given {@link QueryNode}s which are treated as components
     * of a conjunctive query (i.e., the planner will attempt to join them all under a single
     * root).
     *
     * If the join-graph between the given {@link QueryNode}s  is not fully connected,
     * {@link CartesianNode}s will be introduced into the plan, <b>usually</b> as the root.
     *
     * @param query Full query
     * @param fragments set of independent queries associated to sources. Should not contain
     *                  duplicates. Must not be empty
     * @throws IllegalArgumentException if fragments is empty.
     * @return The root of the query plan.
     */
    @Nonnull
    PlanNode plan(@Nonnull CQuery query, @Nonnull Collection<QueryNode> fragments);
}
