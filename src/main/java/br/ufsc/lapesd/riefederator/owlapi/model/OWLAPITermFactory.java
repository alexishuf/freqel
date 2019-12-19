package br.ufsc.lapesd.riefederator.owlapi.model;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLDatatype;

import javax.annotation.Nonnull;

public class OWLAPITermFactory implements TermFactory {
    private @Nonnull final OWLDataFactory dataFactory;

    public OWLAPITermFactory(@Nonnull OWLDataFactory dataFactory) {
        this.dataFactory = dataFactory;
    }

    @Override
    public @Nonnull Blank createBlank() {
        return new OWLAPIAnonymous(dataFactory.getOWLAnonymousIndividual());
    }

    @Override
    public @Nonnull URI createURI(@Nonnull String uri) {
        return new OWLAPINamed(dataFactory.getOWLNamedIndividual(uri));
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
}
