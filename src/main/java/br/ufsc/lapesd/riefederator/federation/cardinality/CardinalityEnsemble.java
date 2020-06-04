package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.federation.cardinality.impl.NoCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import com.google.inject.ProvidedBy;

import javax.annotation.Nonnull;

/**
 * Applies all known {@link CardinalityHeuristic}s and aggregates their results to determine
 * the best estimate.
 */
@ProvidedBy(NoCardinalityEnsemble.SingletonProvider.class)
public interface CardinalityEnsemble {
    @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nonnull TPEndpoint endpoint);
}
