package br.ufsc.lapesd.freqel.model.term;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public abstract class AbstractBlank implements Blank {
    @Override
    public Type getType() {
        return Type.BLANK;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Blank) && getId().equals(((Blank) o).getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        String name = getName();
        return name != null ? "_:" + name : "_:" + getId();
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return toString();
    }
}
