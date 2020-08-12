package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.FreeQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import com.google.common.base.Preconditions;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This is a non-executable arbitrarily-sized join.
 *
 * As part of planning, a join-order is determined and any instance of this node
 * class would then be replaced by a tree of nested {@link JoinOp} instances.
 *
 * This operation could be used to create conjunctions between unions, which cannot be represented
 * in a CQuery instace (and thus cannot be represented by an
 * {@link FreeQueryOp}/{@link QueryOp}.
 */
public class ConjunctionOp extends AbstractInnerOp {
    public static class Builder {
        private @Nonnull List<Op> children = new ArrayList<>();
        private @Nullable List<Modifier> modifiers = null;

        public @Nonnull Builder add(@Nonnull Op op) {
            children.add(op);
            return this;
        }

        public @Nonnull Builder add(@Nonnull Modifier modifier) {
            if (modifiers == null)
                modifiers = new ArrayList<>();
            modifiers.add(modifier);
            return this;
        }

        public @Nonnull Op build() {
            Preconditions.checkState(!children.isEmpty(), "Builder is empty");
            Op op = children.size() > 1 || modifiers != null ? new ConjunctionOp(children)
                                                             : children.get(0);
            if (modifiers != null)
                op.modifiers().addAll(modifiers);
            return op;
        }

    }

    public static @CheckReturnValue @Nonnull Builder builder() {
        return new Builder();
    }

    public static @CheckReturnValue @Nonnull Op build(@Nonnull Collection<Op> children) {
        Preconditions.checkArgument(!children.isEmpty(), "No child Ops given");
        return children.size() > 1 ? new ConjunctionOp(children) : children.iterator().next();
    }

    public ConjunctionOp(@Nonnull Collection<Op> children) {
        super(children);
        assertAllInvariants();
    }

    @Override
    public @Nonnull Op createWith(@Nonnull List<Op> children, @Nullable Collection<Modifier> mods) {
        ConjunctionOp op = new ConjunctionOp(children);
        if (mods != null) op.modifiers().addAll(mods);
        op.setCardinality(getCardinality());
        return op;
    }

    @Override
    protected @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder) {
        return builder.append(getChildren().isEmpty() ? "Empty" : "").append("Conjunction");
    }

    @Override
    protected @Nonnull String toStringSeparator() {
        return " . ";
    }
}
