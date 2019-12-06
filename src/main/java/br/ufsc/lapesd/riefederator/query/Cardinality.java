package br.ufsc.lapesd.riefederator.query;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class Cardinality  {
    public enum Reliability {
        UNSUPPORTED,
        /**
         * A guess on the cardinality, no guarantees
         */
        GUESS,             // less reliable
        /**
         * A lower bound on the results. Number of results is at least as big.
         */
        LOWER_BOUND,
        /**
         * An upper bound, actual number of results will be this or smaller.
         */
        UPPER_BOUND,
        /**
         * Exact number of results
         */
        EXACT              // more reliable
    }

    private final @Nonnull Reliability reliability;
    private final int value;
    public static final @Nonnull Cardinality UNSUPPORTED =
            new Cardinality(Reliability.UNSUPPORTED, -1);

    public Cardinality(@Nonnull Reliability reliability, int value) {
        Preconditions.checkArgument(reliability != Reliability.UNSUPPORTED || value == -1,
                "If reliability is UNSUPPORTED, value must be -1.");
        this.reliability = reliability;
        this.value = value;
    }

    public @Nonnull Reliability getReliability() {
        return reliability;
    }

    /**
     * Get the cardinality value.
     *
     * @param fallback value to return if <code>getReliability()==UNSUPPORTED</code>
     * @return value or fallback
     */
    public int getValue(int fallback) {
        return reliability == Reliability.UNSUPPORTED ? fallback : value;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("%s(%d)", reliability, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cardinality)) return false;
        Cardinality that = (Cardinality) o;
        return value == that.value &&
                getReliability() == that.getReliability();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getReliability(), value);
    }
}
