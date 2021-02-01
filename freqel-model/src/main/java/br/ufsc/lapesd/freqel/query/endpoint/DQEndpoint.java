package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

/**
 * Disjunctive query endpoint. Allows executing OPTIONAL and UNION operations.
 */
public interface DQEndpoint extends CQEndpoint {
    /**
     * Get a profile of supported operations by {@link DQEndpoint#query(Op)}
     */
    @Nonnull DisjunctiveProfile getDisjunctiveProfile();

    /**
     * Executes the given disjunctive query and return a {@link Results} object.
     *
     * @param query the root of the query to answer
     * @return the handle for consuming the results
     * @throws DQEndpointException If the query has endpoints other than this or if this
     *                             endpoint does not support some of the {@link Op} nodes.
     *                             See {@link DQEndpoint#getDisjunctiveProfile()}
     * @throws QueryExecutionException If something goes wrong during query execution.
     */
    @Nonnull Results query(@Nonnull Op query) throws DQEndpointException, QueryExecutionException;
}
