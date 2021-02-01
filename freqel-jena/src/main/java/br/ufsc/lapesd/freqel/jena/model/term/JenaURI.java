package br.ufsc.lapesd.freqel.jena.model.term;

import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.URI;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

@Immutable
public class JenaURI extends JenaRes implements URI {
    private final String uri;

    public JenaURI(@Nonnull RDFNode node) {
        super(node.asResource());
        uri = node.asResource().getURI();
    }

    @Override
    public @Nonnull String getURI() {
        return uri;
    }

    @Override
    public Type getType() {
        return Type.URI;
    }

    @Override
    public String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof URI)) return false;
        if (uri.hashCode() != o.hashCode()) return false;
        return uri.equals(((URI) o).getURI());
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }
}
