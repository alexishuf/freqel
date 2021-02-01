package br.ufsc.lapesd.freqel.cardinality.impl;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.cardinality.CardinalityComparator;
import br.ufsc.lapesd.freqel.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.freqel.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Set;

/**
 * Returns the worst cardinality estimated by any heuristic.
 *
 * {@link Cardinality.Reliability#EXACT} cardinalities have priority: the worst exact
 * cardinality will be returned even if less precise cardinalities with larger value
 * have been found. As a consequence, if some heuristic outputs
 * {@link Cardinality#EMPTY}, that will take precedence over other non-exact estimates.
 */
public class WorstCaseCardinalityEnsemble implements CardinalityEnsemble {
    @Nonnull
    private final CardinalityComparator comparator;
    private final @Nonnull Set<CardinalityHeuristic> heuristicSet;

    @Inject
    public WorstCaseCardinalityEnsemble(@Nonnull CardinalityComparator comparator,
                                        @Nonnull Set<CardinalityHeuristic> heuristicSet) {
        this.comparator = comparator;
        this.heuristicSet = heuristicSet;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, @Nullable TPEndpoint endpoint) {
        Cardinality w = Cardinality.UNSUPPORTED, wExact = Cardinality.UNSUPPORTED;
        for (CardinalityHeuristic heuristic : heuristicSet) {
            Cardinality e = heuristic.estimate(query, endpoint);
            if (e.getReliability() == Cardinality.Reliability.EXACT)
                wExact = max(wExact, e);
            else
                w = max(e, w);
        }
        return wExact.equals(Cardinality.UNSUPPORTED) ? w : wExact;
    }

    private @Nonnull Cardinality max(@Nonnull Cardinality left, @Nonnull Cardinality right) {
        if      ( left.equals(Cardinality.UNSUPPORTED)) return right;
        else if (right.equals(Cardinality.UNSUPPORTED)) return left;
        return comparator.compare(left, right) > 0 ? left : right;
    }
}
