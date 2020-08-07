package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A heuristic for estimating the cardinality of a query at an endpoint.
 *
 * The heuristic <strong>may</strong> include sending a SELECT+LIMIT or ASK query to the source
 * and also <strong>may</strong> involve querying some index.
 */
public interface CardinalityHeuristic {
    @Nonnull Cardinality estimate(@Nonnull CQuery query,
                                  @Nullable TPEndpoint endpoint);
}
