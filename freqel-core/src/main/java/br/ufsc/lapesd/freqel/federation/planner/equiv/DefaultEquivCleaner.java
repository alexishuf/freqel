package br.ufsc.lapesd.freqel.federation.planner.equiv;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.planner.EquivCleaner;

import javax.annotation.Nonnull;
import java.util.Comparator;

public class DefaultEquivCleaner implements EquivCleaner {
    public static final @Nonnull DefaultEquivCleaner INSTANCE = new DefaultEquivCleaner();

    @Override
    public @Nonnull Op cleanEquivalents(@Nonnull Op node, @Nonnull Comparator<Op> comparator) {
        return TreeUtils.cleanEquivalents(node, comparator);
    }
}
