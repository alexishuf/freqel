package br.ufsc.lapesd.freqel.algebra.util;

import br.ufsc.lapesd.freqel.algebra.Cardinality;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import static br.ufsc.lapesd.freqel.algebra.Cardinality.Reliability.*;

public class RelativeCardinalityAdder implements CardinalityAdder {
    private final int nonEmptyMin;
    private final double nonEmptyProportion;
    private final double unsupportedProportion;

    public static final CardinalityAdder DEFAULT = new RelativeCardinalityAdder(1, 0.25, 0.75);

    public static class SingletonProvider implements Provider<CardinalityAdder> {
        @Override
        public CardinalityAdder get() {
            return DEFAULT;
        }
    }

    public RelativeCardinalityAdder(int nonEmptyMin, double nonEmptyProportion, double unsupportedProportion) {
        this.nonEmptyMin = nonEmptyMin;
        this.nonEmptyProportion = nonEmptyProportion;
        this.unsupportedProportion = unsupportedProportion;
    }

    @Override
    public @Nonnull
    Cardinality apply(@Nullable Cardinality left, @Nullable Cardinality right) {
        left  =  left == null ? Cardinality.UNSUPPORTED : left;
        right = right == null ? Cardinality.UNSUPPORTED : right;
        Cardinality.Reliability lr = left.getReliability(), rr = right.getReliability();

        if (lr == UNSUPPORTED && rr == UNSUPPORTED) {
            return Cardinality.UNSUPPORTED;
        } else if (lr == UNSUPPORTED || rr == UNSUPPORTED) {
            if (lr == NON_EMPTY || rr == NON_EMPTY)
                return Cardinality.NON_EMPTY;
            return Cardinality.guess(getValue(left, right) + getValue(right, left));
        } else if (lr.isAtLeast(UPPER_BOUND)
                && rr.isAtLeast(UPPER_BOUND)) {
            Cardinality.Reliability r = lr == rr && lr == EXACT ? EXACT
                    : UPPER_BOUND;
            return new Cardinality(r, left.getValue(0) + right.getValue(0));
        } else if (lr.isAtMost(GUESS) || rr.isAtMost(GUESS)) {
            return Cardinality.guess(getValue(left, right) + getValue(right, left));
        } else {
            assert lr.isAtLeast(LOWER_BOUND) && rr.isAtLeast(LOWER_BOUND);
            assert lr.isAtMost(LOWER_BOUND) && rr.isAtMost(LOWER_BOUND);
            return Cardinality.lowerBound(getValue(left, right) + getValue(right, left));
        }
    }

    private long getValue(@Nonnull Cardinality cardinality, @Nonnull Cardinality other) {
        assert cardinality.getReliability() != UNSUPPORTED
                || other.getReliability() != UNSUPPORTED;
        if (cardinality.getReliability() == NON_EMPTY) {
            return Math.max(nonEmptyMin, (int)(other.getValue(1)*nonEmptyProportion));
        } else if (cardinality.getReliability() == UNSUPPORTED) {
            return (int)(other.getValue(1)*unsupportedProportion);
        }
        long value = cardinality.getValue(-1);
        assert value >= 0;
        return value;
    }
}
