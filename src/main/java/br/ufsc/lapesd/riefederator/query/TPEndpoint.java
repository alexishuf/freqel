package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;

public interface TPEndpoint {
    /**
     * Return a {@link Results} over the possible bindings for each variable in query.
     *
     * If query has no variables (it is fully bound), then the {@link Results} will
     * have no result (<code>hasNext()==false</code>) if the endpoint does not contain the
     * triple. Else, the {@link Results} will return a single {@link Solution} that has
     * no variables bound.
     *
     * @param query triple pattern
     * @return A new {@link Results}
     */
    @Contract("_ -> new")
    @Nonnull Results query(@Nonnull Triple query);

    /**
     * Same as query(Triple), but allows a {@link CQuery} that may have capabilities.
     *
     * @param query The query to process
     * @throws IllegalArgumentException if query.size() != 1
     */
    @Contract("_ -> new")
    @Nonnull Results query(@Nonnull CQuery query);

    /**
     * Indicates whether this endpoint supports the capability in queries given to query().
     */
    boolean hasCapability(@Nonnull Capability capability);
}
