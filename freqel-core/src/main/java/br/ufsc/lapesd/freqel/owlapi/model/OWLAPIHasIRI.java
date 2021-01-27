package br.ufsc.lapesd.freqel.owlapi.model;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.term.URI;
import com.google.common.base.Preconditions;
import org.semanticweb.owlapi.model.HasIRI;
import org.semanticweb.owlapi.model.OWLObject;

import javax.annotation.Nonnull;

public class OWLAPIHasIRI extends OWLAPITerm implements URI {

    public OWLAPIHasIRI(@Nonnull OWLObject object) {
        super(object);
        Preconditions.checkArgument(object instanceof HasIRI,
                "object must be a OWLNamedIndividual");
    }

    @Override
    public @Nonnull String getURI() {
        return ((HasIRI)asOWLObject()).getIRI().toString();
    }

    @Override
    public @Nonnull Type getType() {
        return Type.URI;
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        String uri = getURI();
        return dict.shorten(uri).toString(uri);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof URI) ? getURI().equals(((URI) o).getURI()) : super.equals(o);
    }

    @Override
    public int hashCode() {
        return getURI().hashCode();
    }
}
