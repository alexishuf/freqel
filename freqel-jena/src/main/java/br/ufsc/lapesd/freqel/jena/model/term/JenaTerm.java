package br.ufsc.lapesd.freqel.jena.model.term;

import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaTerm implements JenaBaseTerm, Term {
    @SuppressWarnings("Immutable")
    protected final RDFNode node;
    @SuppressWarnings("Immutable")
    protected final @Nonnull Node graphNode;

    protected JenaTerm(@Nonnull RDFNode node) {
        this.node = node;
        this.graphNode = node.asNode();
    }

    /**
     * Used only by {@link JenaVar}, for which there is no {@link RDFNode} representation.
     */
    protected JenaTerm(@Nonnull Node graphNode) {
        this.node = null;
        this.graphNode = graphNode;
    }

    @Override
    public @Nonnull RDFNode getModelNode() {
        if (node == null)
            throw new UnsupportedOperationException(this +" has no representation as RDFNode");
        return node;
    }

    @Override
    public @Nonnull Node getGraphNode() {
        return graphNode;
    }

    @Override
    public String toString() {
        return node != null ? node.toString() : graphNode.toString();
    }

    @Override
    public int hashCode() {
        return node != null ? node.hashCode() : graphNode.hashCode();
    }
}
