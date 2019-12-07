package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.common.base.Objects;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaTerm implements Term {
    @SuppressWarnings("Immutable")
    private final @Nonnull RDFNode node;

    public JenaTerm(@Nonnull RDFNode node) {
        this.node = node;
    }

    public @Nonnull RDFNode getNode() {
        return node;
    }

    @Override
    public String toString() {
        return node.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getNode());
    }
}
