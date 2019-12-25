package br.ufsc.lapesd.riefederator.federation.tree;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JoinNode extends PlanNode {
    private @Nonnull Set<String> joinVars;
    private @Nonnull PlanNode left, right;

    public static class Builder {
        private @Nonnull PlanNode left, right;
        private @Nullable Set<String> joinVars = null, resultVars = null;
        private boolean projecting = true;

        public Builder(@Nonnull PlanNode left, @Nonnull PlanNode right) {
            this.left = left;
            this.right = right;
        }

        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder addJoinVar(@Nonnull String name) {
            if (joinVars == null) joinVars = new HashSet<>();
            joinVars.add(name);
            return this;
        }
        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder addJoinVars(@Nonnull List<String> names) {
            if (joinVars == null) joinVars = new HashSet<>();
            joinVars.addAll(names);
            return this;
        }

        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder addResultVar(@Nonnull String name) {
            if (resultVars == null) resultVars = new HashSet<>();
            resultVars.add(name);
            return this;
        }
        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder addResultVars(@Nonnull List<String> names) {
            if (resultVars == null) resultVars = new HashSet<>();
            resultVars.addAll(names);
            return this;
        }
        @Contract("_ -> this") @CanIgnoreReturnValue
        public @Nonnull Builder setResultVarsNoProjection(@Nonnull Collection<String> names) {
            resultVars = names instanceof Set ? (Set<String>) names : new HashSet<>(names);
            projecting = false;
            return this;
        }

        public JoinNode build() {
            if (joinVars == null) {
                joinVars = new HashSet<>(left.getResultVars());
                joinVars.retainAll(right.getResultVars());
            } else {
                Preconditions.checkArgument(joinVars.stream()
                        .allMatch(n -> left.getResultVars().contains(n)
                                    && right.getResultVars().contains(n)),
                        "There are join vars which do not occur in some side");
            }
            int capacity = left.getResultVars().size() + right.getResultVars().size();
            HashSet<String> all;
            if (JoinNode.class.desiredAssertionStatus() || resultVars == null) {
                all = new HashSet<>(capacity);
                all.addAll(left.getResultVars());
                all.addAll(right.getResultVars());
                Preconditions.checkArgument(all.containsAll(joinVars), "There are extra joinVars");
                if (resultVars != null) {
                    Preconditions.checkArgument(all.containsAll(resultVars),
                            "There are extra resultVars");
                    Preconditions.checkArgument(projecting == !resultVars.containsAll(all),
                            "Mismatch between projecting and resultVars");
                }
            } else {
                all = null;
            }
            if (resultVars == null) {
                resultVars = all;
                projecting = false;
            }
            return new JoinNode(left, right, joinVars, resultVars, projecting);
        }
    }

    public static @Nonnull Builder builder(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return new Builder(left, right);
    }

    protected JoinNode(@Nonnull PlanNode left, @Nonnull PlanNode right,
                       @Nonnull Set<String> joinVars,
                       @Nonnull Set<String> resultVars, boolean projecting) {
        super(resultVars, projecting);
        this.left = left;
        this.right = right;
        this.joinVars = joinVars;
    }

    public @Nonnull Set<String> getJoinVars() {
        return joinVars;
    }

    public @Nonnull PlanNode getLeft() {
        return left;
    }

    public @Nonnull PlanNode getRight() {
        return right;
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        right.toString(left.toString(builder).append(" ⋈ "));
        if (isProjecting())
            builder.append(')');
        return builder;
    }
}
