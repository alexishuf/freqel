package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class Distinct implements Modifier {
    public static Distinct REQUIRED = new Distinct(true);
    public static Distinct ADVISED = new Distinct(false);

    private final boolean required;

    public Distinct(boolean required) {
        this.required = required;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.DISTINCT;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Distinct)) return false;
        Distinct distinct = (Distinct) o;
        return isRequired() == distinct.isRequired();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isRequired());
    }

    @Override
    public String toString() {
        return required ? "DISTINCT[required]" : "DISTINCT[advised]";
    }
}
