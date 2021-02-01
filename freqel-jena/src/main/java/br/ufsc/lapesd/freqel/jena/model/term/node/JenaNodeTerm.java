package br.ufsc.lapesd.freqel.jena.model.term.node;

import br.ufsc.lapesd.freqel.jena.model.term.JenaBaseTerm;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.LiteralImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaNodeTerm implements JenaBaseTerm, Term {
    @SuppressWarnings("Immutable")
    protected final @Nonnull Node node;

    protected JenaNodeTerm(@Nonnull Node node) {
        this.node = node;
    }

    @Override
    public @Nonnull Node getGraphNode() {
        return node;
    }

    @Override
    public @Nonnull RDFNode getModelNode() {
        Node node = getGraphNode();
        if (node.isLiteral())
            return new LiteralImpl(node, null);
        else if (node.isURI() || node.isURI())
            return new ResourceImpl(node, null);
        throw new UnsupportedOperationException(this+" has no representation as RDFNode");
    }

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }
}
