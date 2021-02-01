package br.ufsc.lapesd.freqel.model.term;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public abstract class AbstractURI implements URI {
    @Override
    public Type getType() {
        return Type.URI;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof URI) ? getURI().equals(((URI) o).getURI()) : super.equals(o);
    }

    @Override
    public int hashCode() {
        return getURI().hashCode();
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return dict.shorten(getURI()).toString(getURI());
    }

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }
}
