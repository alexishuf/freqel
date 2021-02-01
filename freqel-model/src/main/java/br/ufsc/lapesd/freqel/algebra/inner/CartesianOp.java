package br.ufsc.lapesd.freqel.algebra.inner;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.freqel.query.modifiers.Modifier;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CartesianOp extends AbstractInnerOp {

    public static class Builder {
        private final @Nonnull List<Op> list = new ArrayList<>();
        private @Nullable List<Modifier> modifiers = null;

        public @Nonnull Builder add(@Nonnull Op op) {
            list.add(op);
            return this;
        }

        public @Nonnull Builder add(@Nonnull Modifier modifier) {
            if (modifiers == null)
                modifiers = new ArrayList<>();
            modifiers.add(modifier);
            return this;
        }

        public @Nonnull Op build() {
            Preconditions.checkState(!list.isEmpty(), "Builder is empty");
            Op op = list.size() > 1 || modifiers != null ? new CartesianOp(list) : list.get(0);
            if (modifiers != null)
                op.modifiers().addAll(modifiers);
            return op;
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

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
    public  @Nonnull Op createWith(@Nonnull List<Op> children,
                                   @Nullable Collection<Modifier> modifiers) {
        CartesianOp op = new CartesianOp(children);
        if (modifiers != null) op.modifiers().addAll(modifiers);
        op.setCardinality(getCardinality());
        return op;
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
