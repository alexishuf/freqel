package br.ufsc.lapesd.riefederator.rdf.term.std;

import br.ufsc.lapesd.riefederator.rdf.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.rdf.term.URI;
import com.google.common.base.Objects;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class StdURI implements URI {
    private final @Nonnull String uri;

    public StdURI(@Nonnull String uri) {
        this.uri = uri;
    }

    @Override
    public @Nonnull String getURI() {
        return uri;
    }

    @Override
    public Type getType() {
        return Type.URI;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StdURI)) return false;
        StdURI stdURI = (StdURI) o;
        return Objects.equal(uri, stdURI.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uri);
    }

    @Override
    public String toString() {
        return getURI();
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return dict.shorten(uri).toString(uri);
    }
}
