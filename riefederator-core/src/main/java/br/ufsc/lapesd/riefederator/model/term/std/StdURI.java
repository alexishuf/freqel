package br.ufsc.lapesd.riefederator.model.term.std;

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
}
