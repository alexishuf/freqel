package br.ufsc.lapesd.riefederator.jena.model.term.node;

import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.*;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;


@ThreadSafe
public class JenaNodeTermFactory implements TermFactory {
    public static final @Nonnull JenaNodeTermFactory INSTANCE = new JenaNodeTermFactory();

    @Override
    public @Nonnull JenaBlankNode createBlank() {
        return new JenaBlankNode((Node_Blank) NodeFactory.createBlankNode());
    }

    @Override
    public @Nonnull Blank createBlank(String name) {
        Node node = NodeFactory.createBlankNode(BlankNodeId.create(name));
        return new JenaBlankNode((Node_Blank) node, name);
    }

    @Override
    public @Nonnull JenaURINode createURI(@Nonnull String uri) {
        return new JenaURINode((Node_URI) NodeFactory.createURI(uri));
    }

    @Override
    public @Nonnull JenaLitNode createLit(@Nonnull String lexicalForm, @Nonnull String datatypeURI,
                                  boolean escaped) {
        if (escaped) lexicalForm = RDFUtils.unescapeLexicalForm(lexicalForm);
        RDFDatatype dType = TypeMapper.getInstance().getTypeByName(datatypeURI);
        return new JenaLitNode((Node_Literal) NodeFactory.createLiteral(lexicalForm, dType));
    }

    @Override
    public @Nonnull JenaLitNode createLangLit(@Nonnull String lexicalForm, @Nonnull String langTag, boolean escaped) {
        if (escaped) lexicalForm = RDFUtils.unescapeLexicalForm(lexicalForm);
        return new JenaLitNode((Node_Literal) NodeFactory.createLiteral(lexicalForm, langTag));
    }

    @Override
    public @Nonnull JenaVarNode createVar(@Nonnull String name) {
        return new JenaVarNode((Node_Variable)NodeFactory.createVariable(name));
    }

    @Override
    public boolean canCreateVar() {
        return true;
    }
}
