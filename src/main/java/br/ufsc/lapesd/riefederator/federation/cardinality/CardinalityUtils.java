package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Cardinality.Reliability;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Comparator;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.GUESS;
import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.NON_EMPTY;

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

    public static @Nonnull Cardinality multiply(@Nonnull Cardinality c, double factor) {
        Preconditions.checkArgument(factor > 0);
        if (c.getReliability().isAtMost(NON_EMPTY))
            return c;
        int value = c.getValue(-1);
        assert value >= 0;
        return new Cardinality(c.getReliability(), (int)Math.ceil(value *factor));
    }

    public static @Nonnull PlanNode min(@Nonnull CardinalityComparator comparator,
                                        @Nonnull PlanNode l, @Nonnull PlanNode r) {
        int diff = comparator.compare(l.getCardinality(), r.getCardinality());
        return diff <= 0 ? l : r;
    }
    public static @Nonnull PlanNode max(@Nonnull CardinalityComparator comparator,
                                        @Nonnull PlanNode l, @Nonnull PlanNode r) {
        int diff = comparator.compare(l.getCardinality(), r.getCardinality());
        return diff >= 0 ? l : r;
    }
}
