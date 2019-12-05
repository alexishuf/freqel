package br.ufsc.lapesd.riefederator.rdf.jena.term;

import br.ufsc.lapesd.riefederator.rdf.term.Res;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;

@Immutable
public abstract class JenaRes extends JenaTerm implements Res {
    public JenaRes(@Nonnull RDFNode node) {
        super(node.asResource());
    }
}
