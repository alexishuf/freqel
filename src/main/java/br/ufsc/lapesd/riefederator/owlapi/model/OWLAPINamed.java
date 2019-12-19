package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.URI;
import com.google.common.base.Preconditions;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObject;

import javax.annotation.Nonnull;

public class OWLAPINamed extends OWLAPITerm implements URI {

    public OWLAPINamed(@Nonnull OWLObject object) {
        super(object);
        Preconditions.checkArgument(object instanceof OWLNamedIndividual,
                "object must be a OWLNamedIndividual");
    }

    @Override
    public @Nonnull String getURI() {
        return asOWLNamedIndividual().getIRI().toString();
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
