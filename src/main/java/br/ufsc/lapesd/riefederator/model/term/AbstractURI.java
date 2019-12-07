package br.ufsc.lapesd.riefederator.model.term;

import com.google.errorprone.annotations.Immutable;

@Immutable
public abstract class AbstractURI implements URI {
    @Override
    public Type getType() {
        return Type.URI;
    }

    @Override
    public int hashCode() {
        return toNT().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof URI) && toNT().equals(((URI)obj).toNT());
    }
}
