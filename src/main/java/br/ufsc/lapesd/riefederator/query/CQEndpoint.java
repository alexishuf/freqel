package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;

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
    @Nonnull Results query(@Nonnull CQuery query, @Nonnull PrefixDict dict);

    @Override
    @Contract("_ -> new")
    default @Nonnull Results query(@Nonnull CQuery query) {
        return query(query, StdPrefixDict.EMPTY);
    }

    @Override
    @Contract("_ -> new")
    default @Nonnull Results query(@Nonnull Triple query) {
        return query(CQuery.from(query));
    }
}
