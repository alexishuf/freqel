package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static org.apache.jena.rdf.model.ResourceFactory.*;

public class JenaTermFactory implements TermFactory {
    @Override
    public @Nonnull Blank createBlank() {
        return JenaWrappers.fromAnon(createResource());
    }

    @Override
    public @Nonnull URI createURI(@Nonnull String uri) {
        return JenaWrappers.fromURIResource(createResource(uri));
    }

    @Override
    public @Nonnull Lit createLit(@Nonnull String lexicalForm,
                                  @Nonnull String datatypeURI,
                                  boolean escaped) {
        if (escaped) lexicalForm = RDFUtils.unescapeLexicalForm(lexicalForm);
        RDFDatatype dType = TypeMapper.getInstance().getTypeByName(datatypeURI);
        return fromJena(createTypedLiteral(lexicalForm, dType));
    }

    @Override
    public @Nonnull Lit createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag, boolean escaped) {
        if (escaped) lexicalForm = RDFUtils.unescapeLexicalForm(lexicalForm);
        return fromJena(createLangLiteral(lexicalForm, langTag));
    }

    @Override
    public @Nonnull Var createVar(@Nonnull String name) {
        throw new UnsupportedOperationException("Jena does not allow variables in RDF");
    }
}
