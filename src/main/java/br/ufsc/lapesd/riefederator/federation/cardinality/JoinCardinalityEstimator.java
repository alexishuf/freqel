package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.federation.cardinality.impl.BindJoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import com.google.inject.ImplementedBy;

import javax.annotation.Nonnull;

@ImplementedBy(BindJoinCardinalityEstimator.class)
public interface JoinCardinalityEstimator {
    /**
     * Estimates the cardinality of the given join
     *
     * @param info a valid ({@link JoinInfo#isValid()}) join
     * @return estimated cardinality, or {@link Cardinality#UNSUPPORTED}.
     */
    @Nonnull Cardinality estimate(@Nonnull JoinInfo info);
}
