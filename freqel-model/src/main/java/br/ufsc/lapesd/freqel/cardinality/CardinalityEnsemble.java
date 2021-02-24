package br.ufsc.lapesd.freqel.cardinality;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Applies all known {@link CardinalityHeuristic}s and aggregates their results to determine
 * the best estimate.
 */
public interface CardinalityEnsemble {
    @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint);
}
