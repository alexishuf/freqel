package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Conjunctive query endpoint. Any such endpoint is also a {@link TPEndpoint}
 */
public interface CQEndpoint extends TPEndpoint {
    /**
     * Indicates whether {@link CQEndpoint#querySPARQL(String)} is supported
     */
    default boolean canQuerySPARQL() {
        return false;
    }

    /**
     * In the case of endpoints wrapping SPARQL endpoints (or local RDF stores),
     * this will sent the given SPARQL query directly. Results and Solutions will
     * still be handled as if by {@link CQEndpoint#query(CQuery)}.
     *
     * @param sparqlQuery the query
     * @throws UnsupportedOperationException if !{@link CQEndpoint#canQuerySPARQL()}
     * @return Results object
     */
    default @Nonnull Results querySPARQL(@Nonnull String sparqlQuery) {
        throw new UnsupportedOperationException("Cannot handle raw SPARQL queries");
    }

    default @Nonnull Results querySPARQL(@Nonnull String sparqlQuery, boolean isAsk,
                                         @Nonnull Collection<String> varNames) {
        throw new UnsupportedOperationException("Cannot handle raw SPARQL queries");
    }
}
