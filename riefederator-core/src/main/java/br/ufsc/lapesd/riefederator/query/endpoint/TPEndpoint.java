package br.ufsc.lapesd.riefederator.query.endpoint;

import br.ufsc.lapesd.riefederator.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.WebApiEndpoint;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public interface TPEndpoint extends AutoCloseable {
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
     * Estimate the cardinality of results for the given query.
     *
     * The cardinality is an estimate and even higher {@link Cardinality.Reliability}
     * values do not imply the cardinality is correct.
     *
     * Implementations are allowed to perform querying. However, this should only be done
     * if querying will be fast. {@link WebApiEndpoint}, for example, typically will not query.
     *
     * @param query a query that is known to be relevant to this endpoint
     * @param estimatePolicy A {@link EstimatePolicy} with allowed operations
     * @return A {@link Cardinality}, which may be {@link Cardinality#UNSUPPORTED}.
     */
    @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy);

    default @Nonnull Cardinality estimate(@Nonnull CQuery query) {
        return estimate(query, 0);
    }

    /**
     * Get a set of {@link TPEndpoint}s which contain the same data as this one.
     *
     * This is useful to avoid querying the same actual source through
     * multiple different interfaces (e.g., two {@link WebAPICQEndpoint} interfaces to the
     * same data).
     *
     * <b>Implementations MUST be thread safe</b>. One thread is allowed to iterate the
     * {@link Set} returned by this method while another executes
     * {@link TPEndpoint#addAlternatives(Collection)} or
     * {@link TPEndpoint#addAlternative(TPEndpoint)}.
     */
    @Nonnull Set<TPEndpoint> getAlternatives();

    /**
     * Gets the closure of getAlternatives() (recursively calls getAlternatives() on alternatives).
     *
     * This may be cached. Nevertheless, it is safe calling this concurrently with
     * {@link TPEndpoint#addAlternative(TPEndpoint)} or
     * {@link TPEndpoint#addAlternatives(Collection)}.
     */
    @Nonnull Set<TPEndpoint> getAlternativesClosure();

    /**
     * Indicates whether other is a alternative to this endpoint either directly or indirectly.
     *
     * If <code>other==this</code>, returns <code>true</code>.
     */
    boolean isAlternative(@Nonnull TPEndpoint other);

    /**
     * Adds the given endpoints as alternatives to this one.
     * See {@link TPEndpoint#getAlternatives()}
     *
     * <b>Implementations MUST be thread safe</b>. One thread is allowed to execute this method
     * while another iterates the {@link Set} returned by {@link TPEndpoint#getAlternatives()}.
     */
    void addAlternatives(Collection<? extends TPEndpoint> alternatives);

    default void addAlternative(@Nonnull TPEndpoint alternative) {
        addAlternatives(Collections.singletonList(alternative));
    }

    /**
     * Indicates whether this endpoint supports the capability in queries given to query().
     */
    default boolean hasCapability(@Nonnull Capability capability) {
        return hasRemoteCapability(capability);
    }

    default boolean hasSPARQLCapabilities() {
        return false;
    }

    /**
     * Same as hasCapability(), but will only return true if the capability is provided by the
     * remote source.
     */
    boolean hasRemoteCapability(@Nonnull Capability capability);

    /**
     * Some endpoint implementations may hold resources that should be released once the
     * endpoint is not needed anymore.
     *
     * This method should release those resources, but should not propagate any exceptions.
     * If something goes wrong, logging it would be preferred.
     */
    @Override
    default void close() { }
}
