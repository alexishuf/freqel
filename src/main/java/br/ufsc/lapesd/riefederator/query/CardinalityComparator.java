package br.ufsc.lapesd.riefederator.query;

import javax.annotation.Nonnull;
import java.util.Comparator;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.*;

public class CardinalityComparator implements Comparator<Cardinality> {
    private final int large;
    private final int huge;

    public static final @Nonnull CardinalityComparator DEFAULT = new CardinalityComparator();

    public CardinalityComparator() {
        this(256, 2048);
    }

    public CardinalityComparator(int large, int huge) {
        this.large = large;
        this.huge = huge;
    }
    

    @Override
    public int compare(@Nonnull Cardinality l, @Nonnull Cardinality r) {
        if (l.getReliability().equals(r.getReliability()))
            return Integer.compare(l.getValue(0), r.getValue(0));
        if (l.getReliability() == UNSUPPORTED || l.getReliability() == NON_EMPTY)
            return Integer.compare(huge, r.getValue(huge-1));
        if (r.getReliability() == UNSUPPORTED || r.getReliability() == NON_EMPTY)
            return Integer.compare(l.getValue(huge-1), huge);

        if ((l.getReliability() == GUESS || l.getReliability() == LOWER_BOUND) &&
            (r.getReliability() == GUESS || r.getReliability() == LOWER_BOUND)) {
            return Integer.compare(l.getValue(0), r.getValue(0));
        }
        if ((l.getReliability() == UPPER_BOUND || l.getReliability() == EXACT) &&
            (r.getReliability() == UPPER_BOUND || r.getReliability() == EXACT)) {
            return Integer.compare(l.getValue(0), r.getValue(0));
        }
        if (l.getReliability() == UPPER_BOUND || l.getReliability() == EXACT)
            return Integer.compare(l.getValue(0), r.getValue(0)+large);
        if (r.getReliability() == UPPER_BOUND || r.getReliability() == EXACT)
            return Integer.compare(l.getValue(0)+large, r.getValue(0));
        assert false;

        return Integer.compare(l.getValue(0), r.getValue(0));
    }
}
