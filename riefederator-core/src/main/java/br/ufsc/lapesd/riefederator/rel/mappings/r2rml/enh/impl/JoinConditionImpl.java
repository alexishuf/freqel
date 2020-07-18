package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.impl;

import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.RR;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh.JoinCondition;
import br.ufsc.lapesd.riefederator.rel.mappings.r2rml.exceptions.InvalidRRException;
import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.impl.ResourceImpl;

import javax.annotation.Nonnull;

public class JoinConditionImpl extends ResourceImpl implements JoinCondition {
    public static @Nonnull final Implementation factory =
            new ImplementationByProperties(RR.child, RR.parent) {
        @Override
        public EnhNode wrap(Node node, EnhGraph eg) {
            if (!canWrap(node, eg))
                throw new InvalidRRException(node, eg.asGraph(), JoinCondition.class);
            return new JoinConditionImpl(node, eg);
        }
    };

    public JoinConditionImpl(Node n, EnhGraph m) {
        super(n, m);
    }

    @Override
    public @Nonnull String getChild() {
        return RRUtils.getStringNN(this, RR.child);

    }

    @Override
    public @Nonnull String getParent() {
        return RRUtils.getStringNN(this, RR.parent);
    }
}
