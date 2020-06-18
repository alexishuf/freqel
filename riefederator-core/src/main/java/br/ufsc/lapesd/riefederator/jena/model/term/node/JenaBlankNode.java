package br.ufsc.lapesd.riefederator.jena.model.term.node;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Blank;
import org.apache.jena.graph.Node_Blank;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class JenaBlankNode extends JenaNodeTerm implements Blank {
    private final @Nullable String name;

    public JenaBlankNode(@Nonnull Node_Blank node) {
        this(node, null);
    }

    public JenaBlankNode(@Nonnull Node_Blank node, @Nullable String name) {
        super(node);
        this.name = name;
    }
    

    @Override
    public @Nonnull Object getId() {
        return node.getBlankNodeId();
    }

    @Override
    public @Nullable String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return Type.BLANK;
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return name == null ? "[]" : "_:"+name;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Blank) && getId().equals(((Blank) o).getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
