package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This node represents multiple independent queries that output the same projection.
 *
 * There is no need to perform duplicate removals at this stage (hence it is not a Union).
 */
public class UnionOp extends AbstractInnerOp {
    public static class Builder {
        private final List<Op> list = new ArrayList<>();
        private List<Modifier> initialModifiers = null;

        public @CanIgnoreReturnValue @Nonnull Builder add(@Nonnull Op node) {
            list.add(node);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAll(@Nonnull Collection<? extends Op> collection) {
            collection.forEach(this::add);
            return this;
        }

        public @Nonnull Builder add(@Nonnull Modifier modifier) {
            if (initialModifiers == null)
                initialModifiers = new ArrayList<>();
            initialModifiers.add(modifier);
            return this;
        }

        public @CheckReturnValue @Nonnull Op build() {
            Preconditions.checkState(!list.isEmpty(), "Builder is empty");
            Op op = list.size() > 1 || initialModifiers != null ? new UnionOp(list) : list.get(0);
            if (initialModifiers != null)
                op.modifiers().addAll(initialModifiers);
            return op;
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }
    public static @Nonnull Op build(@Nonnull Collection<Op> children) {
        Preconditions.checkArgument(!children.isEmpty(), "No child Ops given");
        return children.size() > 1 ? new UnionOp(children) : children.iterator().next();
    }

    protected UnionOp(@Nonnull Collection<Op> children) {
        super(children);
        setCardinality(children.stream().map(Op::getCardinality)
                .min(ThresholdCardinalityComparator.DEFAULT)
                .orElse(Cardinality.UNSUPPORTED));
        assertAllInvariants();
    }

    @Override
    public @Nonnull Op createWith(@Nonnull List<Op> children, @Nullable Collection<Modifier> mods) {
        UnionOp op = new UnionOp(children);
        op.setCardinality(getCardinality());
        if (mods != null) op.modifiers().addAll(mods);
        return op;
    }

    @Override
    protected @Nonnull String toStringSeparator() {
        return " + ";
    }

    @Override
    protected @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder) {
        return builder.append(getChildren().isEmpty() ? "Empty" : "").append("Union");
    }
}
