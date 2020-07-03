package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@Immutable
public class Limit implements Modifier {
    private final int value;
    private final boolean required;

    public Limit(int value, boolean required) {
        Preconditions.checkArgument(value > 0, "value="+value+" should be >0");
        this.value = value;
        this.required = required;
    }

    public static @Nonnull Limit required(int value) {
        return new Limit(value, true);
    }

    public static @Nonnull Limit advised(int value) {
        return new Limit(value, false);
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.LIMIT;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof Limit)) return false;
        Limit limit = (Limit) o;
        return getValue() == limit.getValue() &&
                isRequired() == limit.isRequired();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue(), isRequired());
    }

    @Override
    public @Nonnull String toString() {
        return String.format("LIMIT[%s] %d", required ? "req" : "adv", value);
    }
}
