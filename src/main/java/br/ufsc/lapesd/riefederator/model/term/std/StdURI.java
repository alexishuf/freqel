package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.AbstractURI;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class StdURI extends AbstractURI {
    private final @Nonnull String uri;

    public StdURI(@Nonnull String uri) {
        this.uri = uri;
    }

    @Override
    public @Nonnull String getURI() {
        return uri;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof StdURI) ? uri.equals(((StdURI) o).uri) : super.equals(o);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return dict.shorten(uri).toString(uri);
    }
}
