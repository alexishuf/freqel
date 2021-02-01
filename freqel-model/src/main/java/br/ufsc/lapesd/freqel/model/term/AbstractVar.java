package br.ufsc.lapesd.freqel.model.term;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.OverridingMethodsMustInvokeSuper;

import javax.annotation.Nonnull;

@Immutable
public abstract class AbstractVar implements Var {
    @Override
    public Type getType() {
        return Type.VAR;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public boolean equals(Object obj) {
        return (obj instanceof Var) && getName().equals(((Var)obj).getName());
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public @Nonnull String toString() {
        return "?"+getName();
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return toString();
    }
}
