package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public class Limit implements Modifier {
    private final int value;

    public Limit(int value) {
        if (value <= 0)
            throw new IllegalArgumentException("value="+value+" should be >0");
        this.value = value;
    }

    public static @Nonnull Limit of(int value) {
        return new Limit(value);
    }

    public int getValue() {
        return value;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.LIMIT;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        return o instanceof Limit && ((Limit)o).getValue() == getValue();
    }

    @Override
    public int hashCode() {
        return 37*getClass().hashCode() + getValue();
    }

    @Override
    public @Nonnull String toString() {
        return "LIMIT "+value;
    }
}
