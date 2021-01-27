package br.ufsc.lapesd.freqel.federation.cardinality;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.federation.cardinality.impl.NoCardinalityEnsemble;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import com.google.inject.ProvidedBy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Applies all known {@link CardinalityHeuristic}s and aggregates their results to determine
 * the best estimate.
 */
@ProvidedBy(NoCardinalityEnsemble.SingletonProvider.class)
public interface CardinalityEnsemble {
    @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint);
}
