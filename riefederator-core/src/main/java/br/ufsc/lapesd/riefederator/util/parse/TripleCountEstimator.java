package br.ufsc.lapesd.riefederator.util.parse;

import javax.annotation.Nonnull;

public interface TripleCountEstimator {
    long estimate(@Nonnull Object source);
    void attachTo(@Nonnull RDFIterationDispatcher dispatcher);
}
