package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.errorprone.annotations.Immutable;
import org.jetbrains.annotations.Contract;
import org.semanticweb.owlapi.model.HasIRI;
import org.semanticweb.owlapi.model.OWLAnonymousIndividual;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.model.OWLObject;

import javax.annotation.Nonnull;

@Immutable
public abstract class OWLAPITerm implements Term {
    @SuppressWarnings("Immutable")
    private final @Nonnull OWLObject object;

    public OWLAPITerm(@Nonnull OWLObject object) {
        this.object = object;
    }

    @Contract("null -> null; !null -> !null")
    public static OWLAPITerm wrap(OWLObject o) {
        if (o == null) return null;
        else if (o instanceof OWLAPITerm) return (OWLAPITerm) o;
        else if (o instanceof OWLAnonymousIndividual) return new OWLAPIAnonymous(o);
        else if (o instanceof HasIRI) return new OWLAPIHasIRI(o);
        else if (o instanceof OWLLiteral) return new OWLAPILit(o);

        String simpleName = o.getClass().getSimpleName();
        throw new IllegalArgumentException("Unsupported OWLObject subclass " + simpleName);
    }

    public @Nonnull OWLObject asOWLObject() {return object;}

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

}
