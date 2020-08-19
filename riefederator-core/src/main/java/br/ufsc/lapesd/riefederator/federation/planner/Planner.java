package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface Planner {
    /**
     * Builds a plan, as a tree, for the given {@link Op}s which are treated as components
     * of a conjunctive query (i.e., the planner will attempt to join them all under a single
     * root).
     *
     * If the join-graph between the given {@link Op}s  is not fully connected,
     * {@link CartesianOp}s will be introduced into the plan, <b>usually</b> as the root.
     *
     * The {@link Op}s given should be either {@link UnionOp}s or {@link EndpointQueryOp}s.
     *
     * @param query Full query
     * @param fragments set of independent queries associated to sources. Should not contain
     *                  duplicates. Must not be empty. Contents should be either
     *                  {@link UnionOp}s or {@link EndpointQueryOp}s
     * @throws IllegalArgumentException if fragments is empty.
     * @return The root of the query plan.
     */
    @Nonnull Op plan(@Nonnull CQuery query, @Nonnull Collection<Op> fragments);
}
