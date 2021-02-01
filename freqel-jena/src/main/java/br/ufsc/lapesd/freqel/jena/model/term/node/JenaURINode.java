package br.ufsc.lapesd.freqel.jena.model.term.node;

import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.URI;
import org.apache.jena.graph.Node_URI;

import javax.annotation.Nonnull;

public class JenaURINode extends JenaNodeTerm implements URI {
    public JenaURINode(@Nonnull Node_URI node) {
        super(node);
    }

    @Override
    public Type getType() {
        return Type.URI;
    }

    @Override
    public @Nonnull String getURI() {
        return node.getURI();
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        String uri = getURI();
        return dict.shorten(uri).toString(uri);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof URI)) return false;
        String uri = getURI();
        if (uri.hashCode() != o.hashCode()) return false;
        return uri.equals(((URI) o).getURI());
    }

    @Override
    public int hashCode() {
        return getURI().hashCode();
    }
}
