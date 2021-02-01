package br.ufsc.lapesd.freqel.cardinality;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.cardinality.impl.ThresholdCardinalityComparator;
import com.google.inject.ProvidedBy;

import javax.annotation.Nonnull;
import java.util.Comparator;

@ProvidedBy(ThresholdCardinalityComparator.SingletonProvider.class)
public interface CardinalityComparator extends Comparator<Cardinality> {

    default @Nonnull Cardinality min(@Nonnull Cardinality l, @Nonnull Cardinality r) {
        return compare(l, r) <= 0 ? l : r;
    }
    default @Nonnull Cardinality max(@Nonnull Cardinality l, @Nonnull Cardinality r) {
        return compare(l, r) >= 0 ? l : r;
    }
}
