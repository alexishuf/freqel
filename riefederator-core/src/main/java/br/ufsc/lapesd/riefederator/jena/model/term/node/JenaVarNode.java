package br.ufsc.lapesd.riefederator.jena.model.term.node;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Var;
import org.apache.jena.graph.Node_Variable;

import javax.annotation.Nonnull;

public class JenaVarNode extends JenaNodeTerm implements Var {
    public JenaVarNode(@Nonnull Node_Variable node) {
        super(node);
    }

    @Override
    public String getName() {
        return node.getName();
    }

    @Override
    public Type getType() {
        return Type.VAR;
    }

    @Override
    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        return "?"+getName();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Var && getName().equals(((Var) obj).getName());
    }

    @Override
    public int hashCode() {
        return node.getName().hashCode();
    }
}
