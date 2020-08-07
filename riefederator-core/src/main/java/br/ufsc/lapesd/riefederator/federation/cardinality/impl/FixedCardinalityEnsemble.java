package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class FixedCardinalityEnsemble implements CardinalityEnsemble {
    private final @Nonnull CardinalityHeuristic heuristic;

    public FixedCardinalityEnsemble(@Nonnull CardinalityHeuristic heuristic) {
        this.heuristic = heuristic;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint) {
        return heuristic.estimate(query, endpoint);
    }
}
