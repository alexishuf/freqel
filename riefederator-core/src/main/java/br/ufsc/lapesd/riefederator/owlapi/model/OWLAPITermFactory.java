package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.*;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import org.semanticweb.owlapi.model.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

public class OWLAPITermFactory implements TermFactory {
    private @Nonnull final OWLDataFactory dataFactory;

    public OWLAPITermFactory(@Nonnull OWLDataFactory dataFactory) {
        this.dataFactory = dataFactory;
    }

    public@Nonnull OWLDataFactory getDF() {
        return dataFactory;
    }

    @Override
    public @Nonnull Blank createBlank() {
        return new OWLAPIAnonymous(dataFactory.getOWLAnonymousIndividual());
    }

    @Override
    public @Nonnull Blank createBlank(String name) {
        return new OWLAPIAnonymous(dataFactory.getOWLAnonymousIndividual(name));
    }

    @Override
    public @Nonnull URI createURI(@Nonnull String uri) {
        return new OWLAPIHasIRI(dataFactory.getOWLNamedIndividual(uri));
    }

    @Override
    public @Nonnull Lit createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI,
                                  boolean escaped) {
        if (escaped)
            lexicalForm = RDFUtils.unescapeLexicalForm(lexicalForm);
        OWLDatatype dt = dataFactory.getOWLDatatype(datatypeURI);
        return new OWLAPILit(dataFactory.getOWLLiteral(lexicalForm, dt));
    }

    @Override
    public @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag,
                                      boolean escaped) {
        if (escaped)
            lexicalForm = RDFUtils.unescapeLexicalForm(lexicalForm);
        return new OWLAPILit(dataFactory.getOWLLiteral(lexicalForm, langTag));
    }

    @Override
    public @Nonnull Var createVar(@Nonnull String name) {
        throw new UnsupportedOperationException("OWLAPI has no variables");
    }

    @Override
    public boolean canCreateVar() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public @Nullable <T> T convertTo(@Nonnull Term term, @Nonnull Class<T> clazz,
                                     @Nonnull Function<String, T> fac) {
        if (term instanceof OWLAPITerm) {
            OWLObject object = ((OWLAPITerm) term).asOWLObject();
            if (clazz.isAssignableFrom(object.getClass())) return (T) object;
        }
        if (term.isURI()) return fac.apply(term.asURI().getURI());
        return null;
    }

    public @Nullable OWLClass convertToOWLClass(@Nonnull Term term) {
        return convertTo(term, OWLClass.class, dataFactory::getOWLClass);
    }
    public @Nullable OWLObjectProperty convertToOWLObjectProperty(@Nonnull Term term) {
        return convertTo(term, OWLObjectProperty.class, dataFactory::getOWLObjectProperty);
    }
    public @Nullable OWLDataProperty convertToOWLDataProperty(@Nonnull Term term) {
        return convertTo(term, OWLDataProperty.class, dataFactory::getOWLDataProperty);
    }
}
