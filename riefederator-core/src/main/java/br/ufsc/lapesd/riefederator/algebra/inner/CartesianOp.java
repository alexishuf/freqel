package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

public class CartesianOp extends AbstractInnerOp {
    private static @Nonnull Cardinality computeCardinality(@Nonnull Collection<Op> children) {
        Cardinality max = children.stream().map(Op::getCardinality)
                .max(ThresholdCardinalityComparator.DEFAULT).orElse(Cardinality.UNSUPPORTED);
        if (max.getReliability().isAtLeast(Cardinality.Reliability.LOWER_BOUND)) {
            long value = max.getValue(Long.MAX_VALUE);
            assert value != Long.MAX_VALUE;
            double pow = Math.pow(value, children.size());
            return new Cardinality(max.getReliability(), (long) Math.min(Long.MAX_VALUE, pow));
        }
        return max;
    }

    public CartesianOp(@Nonnull List<Op> children) {
        super(children);
        setCardinality(computeCardinality(children));
        assertAllInvariants();
    }

    @Override
    protected @Nonnull Op createWith(@Nonnull List<Op> children) {
        return new CartesianOp(children);
    }

    @Override
    protected @Nonnull String toStringSeparator() {
        return " × ";
    }

    @Override
    protected @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder) {
        return builder.append(getChildren().isEmpty() ? "Empty" : "").append('×');
    }
}
