package br.ufsc.lapesd.freqel.query.modifiers;

import br.ufsc.lapesd.freqel.query.endpoint.Capability;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Reasoning implements Modifier {
    public static final @Nonnull Reasoning INSTANCE = new Reasoning();

    protected Reasoning() {}

    @Override public @Nonnull Capability getCapability() {
        return Capability.REASONING;
    }

    @Override public String toString() {
        return "Reason";
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof Reasoning;
    }

    @Override public int hashCode() {
        return getClass().hashCode();
    }
}
