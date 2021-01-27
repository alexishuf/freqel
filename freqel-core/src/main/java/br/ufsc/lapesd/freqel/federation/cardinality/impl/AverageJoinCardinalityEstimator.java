package br.ufsc.lapesd.freqel.federation.cardinality.impl;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.freqel.federation.cardinality.CardinalityUtils;
import br.ufsc.lapesd.freqel.federation.cardinality.JoinCardinalityEstimator;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class AverageJoinCardinalityEstimator implements JoinCardinalityEstimator {
    private final @Nonnull CardinalityComparator comparator;

    @Inject
    public AverageJoinCardinalityEstimator(@Nonnull CardinalityComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull JoinInfo info) {
        if (!info.isValid()) {
            assert false : "Invalid JoinInfo!";
            return Cardinality.UNSUPPORTED;
        }
        Cardinality lc = info.getLeft().getCardinality(), rc = info.getRight().getCardinality();
        if (lc.equals(Cardinality.EMPTY) || rc.equals(Cardinality.EMPTY))
            return Cardinality.EMPTY;
        return CardinalityUtils.worstAvg(comparator, lc, rc);
    }
}
