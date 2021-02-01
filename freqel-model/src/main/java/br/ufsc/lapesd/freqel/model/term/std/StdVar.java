package br.ufsc.lapesd.freqel.model.term.std;

import br.ufsc.lapesd.freqel.model.term.AbstractVar;
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
}
