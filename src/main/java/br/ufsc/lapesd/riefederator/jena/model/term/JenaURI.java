package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.URI;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

@Immutable
public class JenaURI extends JenaRes implements URI {
    public JenaURI(@Nonnull RDFNode node) {
        super(node.asResource());
        Preconditions.checkArgument(node.isURIResource(), "Expected "+node+" to be a URI Resource");
    }

    @Override
    public @Nonnull String getURI() {
        return getNode().asResource().getURI();
    }

    @Override
    public Type getType() {
        return Type.URI;
    }

    @Override
    public String toString() {
        return toTurtle(StdPrefixDict.DEFAULT);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JenaURI) return getNode().equals(((JenaURI) o).getNode());
        return (o instanceof URI) && getURI().equals(((URI) o).getURI());
    }
}
