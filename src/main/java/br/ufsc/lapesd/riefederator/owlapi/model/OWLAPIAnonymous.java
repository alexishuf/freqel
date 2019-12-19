package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import com.google.common.base.Preconditions;
import org.semanticweb.owlapi.model.OWLAnonymousIndividual;
import org.semanticweb.owlapi.model.OWLObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OWLAPIAnonymous extends OWLAPITerm implements Blank {
    public OWLAPIAnonymous(@Nonnull OWLObject object) {
        super(object);
        Preconditions.checkArgument(object instanceof OWLAnonymousIndividual,
                "object must be a OWLAnonymousIndividual");
    }

    @Override
    public @Nonnull Object getId() {
        return asOWLAnonymousIndividual().getID();
    }

    @Override
    public @Nullable String getName() {
        return null;
    }

    @Override
    public Type getType() {
        return Type.BLANK;
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return toString();
    }

    @Override
    public @Nonnull String toString() {
        return "_:"+getId();
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
