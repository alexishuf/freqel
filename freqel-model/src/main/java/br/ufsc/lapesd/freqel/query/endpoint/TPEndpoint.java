package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.cardinality.EstimatePolicy;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.annotations.OverrideAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.Reasoning;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.reason.tbox.EndpointReasoner;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public interface TPEndpoint extends AutoCloseable {
    /**
     * Traverse all decorators and return the underlying {@link TPEndpoint} instance.
     *
     * This should be used if endpoint comparison (by reference) is necessary and one
     * of the operators is not decorated or both operators com from different origins
     * and one of them may be a decorated version of the other.
     *
     * @return effective (non-decorator) {@link TPEndpoint}
     */
    default @Nonnull TPEndpoint getEffective() {
        return this;
    }

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
     * if querying will be fast. WebApiEndpoint, for example, typically will not query.
     *
     * @param query a query that is known to be relevant to this endpoint
     * @param estimatePolicy A {@link EstimatePolicy} with allowed operations
     * @return A {@link Cardinality}, which may be {@link Cardinality#UNSUPPORTED}.
     */
    @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy);

    default @Nonnull Cardinality estimate(@Nonnull CQuery query) {
        return estimate(query, 0);
    }

    @Nonnull Description getDescription();

    /**
     * If this endpoint does not offer the {@link Capability#REASONING} capability it MAY have
     * a preferred implementation of {@link EndpointReasoner} to be used by an OpExecutor.
     *
     * If the endpoint offers the REASONING capability this should be ignored.
     *
     * If the endpoint offers no REASONING capability, returns null here and a query has the
     * {@link Reasoning}} modifier, a the OpExecutor should use a default.
     */
    @Nullable EndpointReasoner getPreferredReasoner();

    /**
     * If true, this endpoint is either backed by an Web API or by something whose input/output
     * relation is similar to what would be observed in an Web API.
     */
    boolean isWebAPILike();

    /**
     * Get a set of {@link TPEndpoint}s which contain the same data as this one.
     *
     * This is useful to avoid querying the same actual source through
     * multiple different interfaces (e.g., two WebAPICQEndpoint interfaces to the
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
     * There are two possibilities of binding a query in a bind join: replacing join variables or
     * annotating the join variables with an {@link OverrideAnnotation}. This method returns true
     * iff the endpoint requires the annotation method to be used.
     */
    boolean requiresBindWithOverride();

    boolean ignoresAtoms();

    /**
     * Return a penalty level for submitting the given query to this endpoint. This method is
     * used to choose the best alternative {@link TPEndpoint} for a single query. The
     * least-penalty endpoint will be chosen.
     */
    double alternativePenalty(@Nonnull CQuery query);

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
