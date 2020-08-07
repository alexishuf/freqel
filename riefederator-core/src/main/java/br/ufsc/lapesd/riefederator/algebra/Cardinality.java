package br.ufsc.lapesd.riefederator.algebra;

import com.google.common.base.Objects;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class Cardinality  {
    private static final Pattern parseRx =
            Pattern.compile("([a-zA-Z_]+)(?:\\((\\d+)\\))?|(\\d+)");

    public enum Reliability {
        UNSUPPORTED,
        /**
         * No cardinality information beyond the knowledge that it is not empty. This differs
         * from LOWER_BOUND by always returning 1 as value.
         */
        NON_EMPTY,
        /**
         * A guess on the cardinality, no guarantees
         */
        GUESS,
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
        EXACT;

        public boolean isAtLeast(@Nonnull Reliability reference) {
            return ordinal() >= reference.ordinal();
        }
        public boolean isAtMost(@Nonnull Reliability reference) {
            return ordinal() <= reference.ordinal();
        }
    }

    private final @Nonnull Reliability reliability;
    private final long value;
    public static final @Nonnull Cardinality
            UNSUPPORTED  = new Cardinality(Reliability.UNSUPPORTED, -1),
            EMPTY = new Cardinality(Reliability.EXACT, 0),
            NON_EMPTY = new Cardinality(Reliability.NON_EMPTY, 1);

    public Cardinality(@Nonnull Reliability reliability, long value) {
        checkArgument(reliability != Reliability.UNSUPPORTED || value == -1,
                "If reliability is UNSUPPORTED, value must be -1.");
        checkArgument(reliability == Reliability.UNSUPPORTED || value != -1,
                "Value cannot be -1 if reliability >UNSUPPORTED");
        this.reliability = reliability;
        if (reliability != Reliability.UNSUPPORTED && value < -1) {
            assert false : "Overflow detected! With asserts disabled, would use MAX_VALUE";
            this.value = Long.MAX_VALUE; //overflow protection
        } else {
            this.value = value;
        }
    }

    public static @Nonnull Cardinality guess(long value) {
        assert value >= 0;
        return new Cardinality(Reliability.GUESS, value);
    }
    public static @Nonnull Cardinality lowerBound(long value) {
        assert value >= 0;
        return new Cardinality(Reliability.LOWER_BOUND, value);
    }
    public static @Nonnull Cardinality upperBound(long value) {
        assert value >= 0;
        return new Cardinality(Reliability.UPPER_BOUND, value);
    }
    public static @Nonnull Cardinality exact(long value) {
        assert value >= 0;
        return new Cardinality(Reliability.EXACT, value);
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
    public long getValue(long fallback) {
        return reliability == Reliability.UNSUPPORTED ? fallback : value;
    }

    public @Nonnull Cardinality decrement() {
        if (getReliability().isAtMost(Reliability.NON_EMPTY)) return this;
        assert value >= 0;
        if (value < 2) return this;
        return new Cardinality(getReliability(), value-1);
    }

    public static @Nullable Cardinality parse(@Nonnull String string) {
        Matcher matcher = parseRx.matcher(string);
        if (!matcher.matches())
            return null;
        if (matcher.group(1) == null) {
            return exact(Integer.parseInt(matcher.group(3)));
        } else {
            for (Reliability v : Reliability.values()) {
                if (v.name().equals(matcher.group(1).toUpperCase())) {
                    if (v == Reliability.UNSUPPORTED) {
                        return UNSUPPORTED;
                    } else if (v == Reliability.NON_EMPTY) {
                        return NON_EMPTY;
                    } else {
                        return matcher.group(2) == null ? null
                                : new Cardinality(v, Integer.parseInt(matcher.group(2)));
                    }
                }
            }
            return null; //bad reliability
        }
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
