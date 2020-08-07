package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
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

        @CanIgnoreReturnValue
        public @Nonnull Builder add(@Nonnull Op node) {
            list.add(node);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder addAll(@Nonnull Op maybeMultiQueryNode) {
            if (maybeMultiQueryNode instanceof UnionOp)
                return addAll(maybeMultiQueryNode.getChildren());
            else
                return add(maybeMultiQueryNode);
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAll(@Nonnull Collection<? extends Op> collection) {
            collection.forEach(this::add);
            return this;
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        @CheckReturnValue
        public @Nonnull Op buildIfMulti() {
            Preconditions.checkState(!isEmpty(), "Builder is empty");
            if (list.size() > 1) return build();
            else return list.get(0);
        }

        @CheckReturnValue
        public @Nonnull UnionOp build() {
            Preconditions.checkState(!isEmpty(), "Builder is empty");
            return new UnionOp(list);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    protected UnionOp(@Nonnull List<Op> children) {
        super(children);
        setCardinality(children.stream().map(Op::getCardinality)
                .min(ThresholdCardinalityComparator.DEFAULT)
                .orElse(Cardinality.UNSUPPORTED));
        assertAllInvariants();
    }

    @Override
    protected @Nonnull Op createWith(@Nonnull List<Op> children) {
        return new UnionOp(children);
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
