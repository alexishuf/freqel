package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaTerm implements Term {
    @SuppressWarnings("Immutable")
    protected final RDFNode node;
    @SuppressWarnings("Immutable")
    protected final @Nonnull Node graphNode;

    public JenaTerm(@Nonnull RDFNode node) {
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

    public @Nonnull RDFNode getNode() {
        if (node == null)
            throw new UnsupportedOperationException(this +" has no representation as RDFNode");
        return node;
    }

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
