package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class Ask implements Modifier {
    public static Ask INSTANCE = new Ask();

    protected Ask() {

    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.ASK;
    }

    @Override
    public @Nonnull String toString() {
        return "ASK";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Ask;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
