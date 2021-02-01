package br.ufsc.lapesd.freqel.jena.model.term;

import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.term.Blank;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.factory.TermFactory;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;
import static org.apache.jena.rdf.model.ResourceFactory.*;

public class JenaTermFactory implements TermFactory {
    public static @Nonnull JenaTermFactory INSTANCE = new JenaTermFactory();

    @Override
    public @Nonnull Blank createBlank() {
        return JenaWrappers.fromAnon(createResource());
    }

    @Override
    public @Nonnull Blank createBlank(String name) {
        Node node = NodeFactory.createBlankNode(BlankNodeId.create(name));
        return new JenaBlank(new ResourceImpl(node, null), name);
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
        return new JenaVar(NodeFactory.createVariable(name));
    }

    @Override
    public boolean canCreateVar() {
        return true;
    }
}
