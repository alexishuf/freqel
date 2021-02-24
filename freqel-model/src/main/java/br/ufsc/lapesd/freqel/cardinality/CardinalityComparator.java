package br.ufsc.lapesd.freqel.cardinality;

import br.ufsc.lapesd.freqel.algebra.Cardinality;

import javax.annotation.Nonnull;
import java.util.Comparator;

public interface CardinalityComparator extends Comparator<Cardinality> {

    default @Nonnull Cardinality min(@Nonnull Cardinality l, @Nonnull Cardinality r) {
        return compare(l, r) <= 0 ? l : r;
    }
    default @Nonnull Cardinality max(@Nonnull Cardinality l, @Nonnull Cardinality r) {
        return compare(l, r) >= 0 ? l : r;
    }
}
