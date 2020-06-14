package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class PatternPredicate implements Predicate<String> {
    private final @Nonnull Pattern pattern;

    public PatternPredicate(@Nonnull Pattern pattern) {
        this.pattern = pattern;
    }

    public PatternPredicate(@Nonnull String rx) {
        this(Pattern.compile(rx));
    }

    @Override
    public boolean test(String s) {
        return pattern.matcher(s).matches();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PatternPredicate)) return false;
        PatternPredicate that = (PatternPredicate) o;
        return pattern.pattern().equals(that.pattern.pattern());
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public @Nonnull String toString() {
        return pattern.toString();
    }
}
