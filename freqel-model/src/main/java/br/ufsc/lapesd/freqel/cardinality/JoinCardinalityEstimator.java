package br.ufsc.lapesd.freqel.cardinality;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.JoinInfo;

import javax.annotation.Nonnull;

public interface JoinCardinalityEstimator {
    /**
     * Estimates the cardinality of the given join
     *
     * @param info a valid ({@link JoinInfo#isValid()}) join
     * @return estimated cardinality, or {@link Cardinality#UNSUPPORTED}.
     */
    @Nonnull Cardinality estimate(@Nonnull JoinInfo info);
}
