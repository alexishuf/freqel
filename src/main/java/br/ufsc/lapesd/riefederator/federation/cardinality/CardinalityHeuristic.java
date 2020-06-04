package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;

/**
 * A heuristic for estimating the cardinality of a query at an endpoint.
 *
 * The heuritic <strong>may</strong> include sending a SELECT+LIMIT or ASK query to the source
 * and also <strong>may</strong> involve querying some index.
 */
public interface CardinalityHeuristic {
    @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nonnull TPEndpoint endpoint);
}
