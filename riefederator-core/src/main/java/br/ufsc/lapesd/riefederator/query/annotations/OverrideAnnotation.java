package br.ufsc.lapesd.riefederator.query.annotations;

import br.ufsc.lapesd.riefederator.model.term.Term;

import javax.annotation.Nonnull;
import java.util.Objects;

public class OverrideAnnotation implements TermAnnotation {
    private final @Nonnull Term overrideValue;

    public OverrideAnnotation(@Nonnull Term overrideValue) {
        this.overrideValue = overrideValue;
    }

    public @Nonnull Term getValue() {
        return overrideValue;
    }

    @Override public String toString() {
        return "Override{"+overrideValue.toString()+"}";
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OverrideAnnotation)) return false;
        OverrideAnnotation that = (OverrideAnnotation) o;
        return overrideValue.equals(that.overrideValue);
    }

    @Override public int hashCode() {
        return Objects.hash(overrideValue);
    }
}
