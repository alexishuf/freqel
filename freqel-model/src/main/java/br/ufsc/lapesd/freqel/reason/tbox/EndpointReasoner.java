package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import java.util.function.Function;

public interface EndpointReasoner {
    /**
     * Offer a TBox for use during future reasoning operations.
     * @param tBox the {@link TBox} instance
     */
    void setTBox(@Nonnull TBox tBox);

    /**
     * Indicates whether this implementation accepts {@link DQueryOp} instances in addition
     * to {@link EndpointQueryOp} instances.
     */
    boolean acceptDisjunctive();

    /**
     * Provide reasoning-derived results for the given query targeted towards the given endpoint
     *
     * @param op The (disjunctive) query that must be executed, associated to a endpoint.
     *           All solutions in this query must be present in the {@link Results} object
     *           output by this method. This will only be a instance {@link DQueryOp}
     *           if {@link #acceptDisjunctive()} is true, else it will be a {@link EndpointQueryOp}
     * @param executor execute a query against a endpoint (without reasoning)
     * @return a {@link Results} with all original solutions and possibly new derived solutions.
     */
    @Nonnull Results apply(@Nonnull EndpointOp op,
                           @Nonnull Function<EndpointOp, Results> executor);
}
