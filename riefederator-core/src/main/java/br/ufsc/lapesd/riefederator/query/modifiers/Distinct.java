package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class Distinct implements Modifier {
    public static Distinct INSTANCE = new Distinct();

    protected Distinct() { }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.DISTINCT;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Distinct;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return "DISTINCT";
    }
}
