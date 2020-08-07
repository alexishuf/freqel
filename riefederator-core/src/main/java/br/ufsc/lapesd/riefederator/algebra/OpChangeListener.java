package br.ufsc.lapesd.riefederator.algebra;

import javax.annotation.Nonnull;

public interface OpChangeListener {
    void matchedTriplesChanged(@Nonnull Op op);
    void varsChanged(@Nonnull Op op);
}
