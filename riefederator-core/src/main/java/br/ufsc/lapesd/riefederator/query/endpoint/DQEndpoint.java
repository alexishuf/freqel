package br.ufsc.lapesd.riefederator.query.endpoint;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;

/**
 * Disjunctive query endpoint. Allows executing OPTIONAL and UNION operations.
 */
public interface DQEndpoint extends CQEndpoint {
    /**
     * Indicates whether this endpoint can answer the query without throwing a
     * {@link DQEndpointException}
     *
     * @param query the root of the query to check
     * @return true if the query is supported
     */
    boolean canQuery(@Nonnull Op query);

    /**
     * Executes the given disjunctive query and return a {@link Results} object.
     *
     * @param query the root of the query to answer
     * @return the handle for consuming the results
     * @throws DQEndpointException If the query has endpoints other than this or if this
     *                             endpoint does not support some of the {@link Op} nodes.
     *                             See {@link DQEndpoint#canQuery(Op)}
     * @throws QueryExecutionException If something goes wrong during query execution.
     */
    @Nonnull Results query(@Nonnull Op query) throws DQEndpointException, QueryExecutionException;
}
