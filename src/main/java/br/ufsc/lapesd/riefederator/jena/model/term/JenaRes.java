package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Res;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaRes extends JenaTerm implements Res {
    public JenaRes(@Nonnull RDFNode node) {
        super(node.asResource());
    }

    public @Nonnull Resource getResource() {
        return getNode().asResource();
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        Resource r = getResource();
        return r.isAnon() ? r.toString() : dict.shorten(r.getURI()).toString(r.getURI());
    }
}
