package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.AbstractVar;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class StdVar extends AbstractVar {
    private final @Nonnull String name;

    public StdVar(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public @Nonnull String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return Type.VAR;
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
