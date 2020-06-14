package br.ufsc.lapesd.riefederator.jena.model.term;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Var;
import org.apache.jena.graph.Node;

import javax.annotation.Nonnull;

public class JenaVar extends JenaTerm implements Var {
    public JenaVar(@Nonnull Node node) {
        super(node);
    }

    @Override
    public @Nonnull String getName() {
        return graphNode.getName();
    }

    @Override
    public @Nonnull Type getType() {
        return Type.VAR;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Var) && getName().equals(((Var) obj).getName());
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return "?"+getName();
    }
}
