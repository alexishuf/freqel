package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.term.AbstractURI;
import com.google.errorprone.annotations.Immutable;
import org.semanticweb.owlapi.model.IRI;

import javax.annotation.Nonnull;

@Immutable
public class OWLAPIIRI extends AbstractURI {
    @SuppressWarnings("Immutable")
    private final @Nonnull IRI iri;

    public OWLAPIIRI(@Nonnull IRI iri) {
        this.iri = iri;
    }

    @Override
    public @Nonnull String getURI() {
        return iri.toString();
    }
}
