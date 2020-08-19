package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class Optional implements Modifier {
    public static final Optional INSTANCE = new Optional();

    protected Optional() { }

    @Override public @Nonnull Capability getCapability() {
        return Capability.OPTIONAL;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Optional;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public @Nonnull String toString() {
        return "OPTIONAL";
    }
}
