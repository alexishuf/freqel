package br.ufsc.lapesd.freqel.cardinality.impl;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.freqel.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

public class FixedCardinalityEnsemble implements CardinalityEnsemble {
    private final @Nonnull CardinalityHeuristic heuristic;

    @Inject public FixedCardinalityEnsemble(@Named("fast") @Nonnull CardinalityHeuristic heuristic) {
        this.heuristic = heuristic;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint) {
        return heuristic.estimate(query, endpoint);
    }
}
