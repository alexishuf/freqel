package br.ufsc.lapesd.riefederator.model.jena.term;

import br.ufsc.lapesd.riefederator.model.term.Blank;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public class JenaBlank extends JenaRes implements Blank {
    private final  @Nullable String name;

    public JenaBlank(@Nonnull RDFNode node, @Nullable String name) {
        super(node.asResource());
        Preconditions.checkArgument(node.isAnon(), "Expected "+node+" to be a blank node");
        this.name = name;
    }

    public JenaBlank(@Nonnull RDFNode node) {
        this(node, null);
    }

    @Override
    public @Nonnull Object getId() {
        return getNode().asResource().getId();
    }

    @Override
    public @Nullable String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return Type.BLANK;
    }
}
