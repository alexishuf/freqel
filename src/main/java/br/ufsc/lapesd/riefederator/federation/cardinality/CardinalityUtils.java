package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Cardinality.Reliability;

import javax.annotation.Nonnull;
import java.util.Comparator;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.GUESS;

public class CardinalityUtils {
    public static @Nonnull Cardinality worstAvg(@Nonnull Comparator<Cardinality> comparator,
                                                @Nonnull Cardinality l, @Nonnull Cardinality r) {
        int diff = comparator.compare(l, r);
        Reliability lr = l.getReliability(), rr = r.getReliability();
        if (lr.isAtLeast(GUESS) && rr.isAtLeast(GUESS)) {
            int lv = l.getValue(-1), rv = r.getValue(-1);
            assert lv >= 0;
            assert rv >= 0;
            return new Cardinality(diff >= 0 ? lr : rr, (int)Math.ceil((lv+rv)/2.0));
        }
        return diff >= 0 ? l : r;
    }
}
