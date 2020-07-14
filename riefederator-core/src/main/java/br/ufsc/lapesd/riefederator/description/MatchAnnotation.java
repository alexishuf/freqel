package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class MatchAnnotation implements TripleAnnotation {
    private final @Nonnull Triple matched;

    public MatchAnnotation(@Nonnull Triple matched) {
        this.matched = matched;
    }

    public @Nonnull Triple getMatched() {
        return matched;
    }

    @Override
    public @Nonnull String toString() {
        return "Matched("+getMatched()+")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MatchAnnotation)) return false;
        MatchAnnotation that = (MatchAnnotation) o;
        return getMatched().equals(that.getMatched());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMatched());
    }
}
