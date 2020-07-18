package br.ufsc.lapesd.riefederator.description.molecules.tags;

import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a possible value bound to this atom.
 *
 * An atom may have multiple possible values and at runtime, it is not guaranteed all
 * observed values will be one of the tagged ones.
 */
@Immutable
public class ValueTag implements AtomTag {
    private final @Nonnull Term value;

    public ValueTag(@Nonnull Term value) {
        this.value = value;
    }

    public static @Nonnull ValueTag of(@Nonnull Term value) {
        return new ValueTag(value);
    }

    public @Nonnull Term getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ValueTag)) return false;
        ValueTag valueTag = (ValueTag) o;
        return getValue().equals(valueTag.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue());
    }

    @Override
    public String toString() {
        return "value="+value;
    }
}
