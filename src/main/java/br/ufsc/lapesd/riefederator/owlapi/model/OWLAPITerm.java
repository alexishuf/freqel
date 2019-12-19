package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.errorprone.annotations.Immutable;
import org.jetbrains.annotations.Contract;
import org.semanticweb.owlapi.model.*;

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
        else if (o instanceof OWLNamedIndividual) return new OWLAPINamed(o);
        else if (o instanceof OWLLiteral) return new OWLAPILit(o);

        String simpleName = o.getClass().getSimpleName();
        throw new IllegalArgumentException("Unsupported OWLObject subclass " + simpleName);
    }

    public @Nonnull OWLObject asOWLObject() {return object;}
    public @Nonnull OWLNamedIndividual asOWLNamedIndividual() {return (OWLNamedIndividual) object;}
    public @Nonnull OWLAnonymousIndividual asOWLAnonymousIndividual() {return (OWLAnonymousIndividual) object;}
    public @Nonnull OWLClass asOWLClass() {return (OWLClass)object;}
    public @Nonnull OWLProperty asOWLProperty() {return (OWLProperty)object;}
    public @Nonnull OWLObjectProperty asOWLObjectProperty() {return (OWLObjectProperty)object;}
    public @Nonnull OWLDataProperty asOWLDataProperty() {return (OWLDataProperty)object;}
    public @Nonnull OWLAnnotationProperty asOWLAnnotationProperty() {return (OWLAnnotationProperty)object;}
    public @Nonnull OWLLiteral asOWLLiteral() {return (OWLLiteral)object;}

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

}
