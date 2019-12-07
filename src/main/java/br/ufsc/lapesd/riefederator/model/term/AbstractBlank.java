package br.ufsc.lapesd.riefederator.model.term;

import com.google.errorprone.annotations.Immutable;

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
}
