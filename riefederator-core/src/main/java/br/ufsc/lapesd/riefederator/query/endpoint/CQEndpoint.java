package br.ufsc.lapesd.riefederator.query.endpoint;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.results.Results;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Conjunctive query endpoint. Any such endpoint is also a {@link TPEndpoint}
 */
public interface CQEndpoint extends TPEndpoint {
    /**
     * Execute a join between all triple patterns in the query. May use @prefix from the
     * given {@link PrefixDict} if allowed by the underlying protocol/interface.
     *
     * This is the same as pushing joins between each of the {@link Results} for each triple
     * in query to the underlying source. Fully bound queries (no variables, aka ASK queries)
     * are handled analogously to how they are in {@link TPEndpoint}, except now the simultaneous
     * presence of all triples is required to return a non-empty {@link Results}.
     *
     * @param query a conjunctive query (a BGP)
     * @param dict A {@link PrefixDict} from which to generate @prefix definitions
     * @return An {@link Results} iterator
     */
    @Contract("_, _ -> new")
    default @Nonnull Results query(@Nonnull CQuery query, @Nonnull PrefixDict dict) {
        return query(query.withPrefixDict(dict));
    }

    @Override
    @Contract("_ -> new")
    @Nonnull Results query(@Nonnull CQuery query);

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

    @Override
    @Contract("_ -> new")
    default @Nonnull Results query(@Nonnull Triple query) {
        return query(CQuery.from(query));
    }
}
