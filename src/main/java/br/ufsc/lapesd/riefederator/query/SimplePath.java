package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.*;

@Immutable
public class SimplePath {
    @Immutable
    public static class Segment {
        private final @Nonnull Term term;
        private final boolean reverse;

        public Segment(@Nonnull Term term) {
            this(term, false);
        }

        public Segment(@Nonnull Term term, boolean reverse) {
            this.term = term;
            this.reverse = reverse;
        }

        public @Nonnull Term getTerm() {
            return term;
        }

        public boolean isReverse() {
            return reverse;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Segment)) return false;
            Segment segment = (Segment) o;
            return reverse == segment.reverse &&
                    term.equals(segment.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, reverse);
        }

        @Override
        public @Nonnull String toString() {
            return toString(StdPrefixDict.DEFAULT);
        }

        public @Nonnull String toString(@Nonnull PrefixDict prefixDict) {
            return (reverse ? "^" : "")+term.toString(prefixDict);
        }
    }

    private final @Nonnull ImmutableList<Segment> segments;
    private @SuppressWarnings("Immutable") @LazyInit int hash = 0;

    public static final @Nonnull SimplePath EMPTY = new SimplePath(Collections.emptyList());

    public SimplePath(@Nonnull List<Segment> segments) {
        this.segments = ImmutableList.copyOf(segments);
    }

    public static @Nonnull SimplePath fromTerms(List<Term> list) {
        //noinspection UnstableApiUsage
        ImmutableList.Builder<Segment> b = ImmutableList.builderWithExpectedSize(list.size());
        for (Term term : list)
            b.add(new Segment(term));
        return new SimplePath(b.build());
    }

    public static @Nonnull SimplePath fromTerms(@Nonnull Term... list) {
        return fromTerms(Arrays.asList(list));
    }

    public static @Nonnull Builder to(@Nonnull Term edge) {
        return new Builder().to(edge);
    }

    public static @Nonnull Builder from(@Nonnull Term edge) {
        return new Builder().from(edge);
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private @Nonnull List<Segment> segments = new ArrayList<>();

        public @Nonnull Builder to(@Nonnull Term edge) {
            segments.add(new Segment(edge));
            return this;
        }

        public @Nonnull Builder from(@Nonnull Term edge) {
            segments.add(new Segment(edge, true));
            return this;
        }

        public @Nonnull SimplePath build() {
            return segments.isEmpty() ? EMPTY : new SimplePath(segments);
        }
    }

    public boolean isSingle() {
        return segments.size() == 1;
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public int size() {
        return segments.size();
    }

    public @Nonnull List<Segment> getSegments() {
        return segments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimplePath)) return false;
        SimplePath that = (SimplePath) o;
        return segments.equals(that.segments);
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = segments.hashCode();
        return hash;
    }

    @Override
    public @Nonnull String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

    public @Nonnull String toString(@Nonnull PrefixDict prefixDict) {
        StringBuilder b = new StringBuilder();
        for (Segment s : segments) {
            b.append(s.toString(prefixDict)).append("/");
        }
        if (!segments.isEmpty())
            b.setLength(b.length()-1);
        return b.toString();
    }
}
