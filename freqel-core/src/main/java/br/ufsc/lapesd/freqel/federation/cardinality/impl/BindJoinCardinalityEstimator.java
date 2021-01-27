package br.ufsc.lapesd.freqel.federation.cardinality.impl;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.freqel.federation.cardinality.JoinCardinalityEstimator;
import br.ufsc.lapesd.freqel.query.CQuery;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.union;

/**
 * A {@link JoinCardinalityEstimator} that simulates the effect of a bidn join to re-estimate
 * the cardinality of the right operand before estimating the join cardinality.
 *
 * Some heuristics are also applied when estiamting the join cardinality.
 */
public class BindJoinCardinalityEstimator implements JoinCardinalityEstimator {
    private final @Nonnull CardinalityEnsemble cardinalityEnsemble;

    @Inject
    public BindJoinCardinalityEstimator(@Nonnull CardinalityEnsemble cardinalityEnsemble) {
        this.cardinalityEnsemble = cardinalityEnsemble;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull JoinInfo i) {
        Op l = i.getLeft(), r = i.getRight();
        CQuery merged = CQuery.from(union(l.getMatchedTriples(), r.getMatchedTriples()));
        return cardinalityEnsemble.estimate(merged, null);
    }
}
