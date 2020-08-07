package br.ufsc.lapesd.riefederator.federation.cardinality;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.DefaultInnerCardinalityComputer;
import com.google.inject.ImplementedBy;

import javax.annotation.Nonnull;

@ImplementedBy(DefaultInnerCardinalityComputer.class)
public interface InnerCardinalityComputer {
    @Nonnull Cardinality compute(JoinOp node);
    @Nonnull Cardinality compute(CartesianOp node);
    @Nonnull Cardinality compute(UnionOp node);

    default @Nonnull Cardinality compute(Op node) {
        if (node instanceof JoinOp)
            return compute((JoinOp)node);
        else if (node instanceof CartesianOp)
            return compute((CartesianOp) node);
        else if (node instanceof UnionOp)
            return compute((UnionOp) node);
        return node.getCardinality();
    }
}
