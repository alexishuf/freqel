package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Estimates cardinality by delegation to {@link TPEndpoint#estimate(CQuery, int)}.
 *
 * This will incurr ASK and LIMIT queries to the endpoint as allowed by the estimatePolicy.
 */
public class LimitCardinalityHeuristic implements CardinalityHeuristic {
    private final int policy;

    public LimitCardinalityHeuristic(int policy) {
        this.policy = policy;
    }

    public int getPolicy() {
        return policy;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint) {
        if (endpoint == null) return Cardinality.UNSUPPORTED;
        return endpoint.estimate(query, policy);
    }
}
