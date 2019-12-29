package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface Planner {
    /**
     * Builds a plan, as a tree, for the given {@link QueryNode}s which are treated as components
     * of a conjunctive query (i.e., the planner will attempt to join them all under a single
     * root).
     *
     * If the join-graph between the given {@link QueryNode}s  is not fully connected,
     * {@link CartesianNode}s will be introduced into the plan, <b>usually</b> as the root.
     *
     * @param conjunctiveQueries set of independent queries. Should not contain duplicates
     *                           Must not be empty
     * @throws IllegalArgumentException if conjunctiveQueries is empty.
     * @return The root of the query plan.
     */
    @Nonnull PlanNode plan(@Nonnull Collection<QueryNode> conjunctiveQueries);
}