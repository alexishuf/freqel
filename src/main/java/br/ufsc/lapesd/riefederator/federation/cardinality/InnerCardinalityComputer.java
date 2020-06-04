package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.federation.cardinality.impl.DefaultInnerCardinalityComputer;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import com.google.inject.ImplementedBy;

import javax.annotation.Nonnull;

@ImplementedBy(DefaultInnerCardinalityComputer.class)
public interface InnerCardinalityComputer {
    @Nonnull Cardinality compute(JoinNode node);
    @Nonnull Cardinality compute(CartesianNode node);
    @Nonnull Cardinality compute(MultiQueryNode node);

    default @Nonnull Cardinality compute(PlanNode node) {
        if (node instanceof JoinNode)
            return compute((JoinNode)node);
        else if (node instanceof CartesianNode)
            return compute((CartesianNode) node);
        else if (node instanceof MultiQueryNode)
            return compute((MultiQueryNode) node);
        return node.getCardinality();
    }
}
