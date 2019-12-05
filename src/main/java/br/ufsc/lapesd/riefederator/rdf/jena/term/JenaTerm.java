package br.ufsc.lapesd.riefederator.rdf.jena.term;

import br.ufsc.lapesd.riefederator.rdf.term.Term;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JenaTerm)) return false;
        JenaTerm jenaTerm = (JenaTerm) o;
        return Objects.equal(getNode(), jenaTerm.getNode());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getNode());
    }
}
