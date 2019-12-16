package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class Ask implements Modifier {
    public static Ask REQUIRED = new Ask(true);
    public static Ask ADVISED = new Ask(false);
    private final boolean required;

    public Ask(boolean required) {
        this.required = required;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.ASK;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public @Nonnull String toString() {
        return "ASK" + (isRequired() ? "[required]" : "[advised]");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Ask)) return false;
        Ask ask = (Ask) o;
        return isRequired() == ask.isRequired();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isRequired());
    }
}
