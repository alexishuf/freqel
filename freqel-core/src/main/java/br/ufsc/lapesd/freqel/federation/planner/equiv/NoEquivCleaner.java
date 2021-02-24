package br.ufsc.lapesd.freqel.federation.planner.equiv;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.planner.EquivCleaner;

import javax.annotation.Nonnull;
import java.util.Comparator;

public class NoEquivCleaner implements EquivCleaner {
    public static final NoEquivCleaner INSTANCE = new NoEquivCleaner();

    @Override
    public @Nonnull Op cleanEquivalents(@Nonnull Op node, @Nonnull Comparator<Op> comparator) {
        return node;
    }
}
