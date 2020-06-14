package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.query.Cardinality;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.*;

public class ThresholdCardinalityComparator implements CardinalityComparator {
    private final int large;
    private final int huge;

    public static final @Nonnull ThresholdCardinalityComparator DEFAULT = new ThresholdCardinalityComparator();

    public static class SingletonProvider implements Provider<CardinalityComparator> {
        @Override
        public @Nonnull ThresholdCardinalityComparator get() {
            return DEFAULT;
        }
    }

    public ThresholdCardinalityComparator() {
        this(256, 2048);
    }

    public ThresholdCardinalityComparator(int large, int huge) {
        this.large = large;
        this.huge = huge;
    }
    

    @Override
    public int compare(@Nonnull Cardinality l, @Nonnull Cardinality r) {
        if (l.getReliability().equals(r.getReliability()))
            return Long.compare(l.getValue(0), r.getValue(0));
        if (l.getReliability() == UNSUPPORTED || l.getReliability() == NON_EMPTY)
            return Long.compare(huge, r.getValue(huge-1));
        if (r.getReliability() == UNSUPPORTED || r.getReliability() == NON_EMPTY)
            return Long.compare(l.getValue(huge-1), huge);

        if ((l.getReliability() == GUESS || l.getReliability() == LOWER_BOUND) &&
            (r.getReliability() == GUESS || r.getReliability() == LOWER_BOUND)) {
            return Long.compare(l.getValue(0), r.getValue(0));
        }
        if ((l.getReliability() == UPPER_BOUND || l.getReliability() == EXACT) &&
            (r.getReliability() == UPPER_BOUND || r.getReliability() == EXACT)) {
            return Long.compare(l.getValue(0), r.getValue(0));
        }
        if (l.getReliability() == UPPER_BOUND || l.getReliability() == EXACT)
            return Long.compare(l.getValue(0), r.getValue(0)+large);
        if (r.getReliability() == UPPER_BOUND || r.getReliability() == EXACT)
            return Long.compare(l.getValue(0)+large, r.getValue(0));
        assert false;

        return Long.compare(l.getValue(0), r.getValue(0));
    }
}
