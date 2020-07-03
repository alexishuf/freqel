package br.ufsc.lapesd.riefederator.jena.model.term.node;

import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.graph.Node;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaNodeTerm implements Term {
    @SuppressWarnings("Immutable")
    protected final @Nonnull Node node;

    protected JenaNodeTerm(@Nonnull Node node) {
        this.node = node;
    }

    public @Nonnull Node getNode() {
        return node;
    }

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }
}
