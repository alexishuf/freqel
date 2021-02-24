package br.ufsc.lapesd.freqel.federation.planner;

import br.ufsc.lapesd.freqel.algebra.Op;

import javax.annotation.Nonnull;
import java.util.Comparator;

public interface EquivCleaner {
    @Nonnull Op cleanEquivalents(@Nonnull Op node, @Nonnull Comparator<Op> comparator);
}
