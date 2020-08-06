package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

public class JoinNode extends AbstractInnerPlanNode {
    private @Nonnull final ImmutableSet<String> joinVars, reqInputs, optInputs;

    public static class Builder {
        private @Nonnull final PlanNode left, right;
        private @Nullable Set<String> joinVars = null, resultVars = null;
        private Cardinality cardinality = null;

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
        public @Nonnull Builder addJoinVars(@Nonnull Collection<String> names) {
            if (joinVars == null) joinVars = new HashSet<>();
            joinVars.addAll(names);
            return this;
        }

        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder setJoinVars(@Nonnull Collection<String> names) {
            joinVars = names instanceof Set ? (Set<String>)names : new HashSet<>(names);
            return this;
        }

        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder addResultVar(@Nonnull String name) {
            if (resultVars == null) resultVars = new HashSet<>();
            resultVars.add(name);
            return this;
        }
        @Contract("_ -> this")  @CanIgnoreReturnValue
        public @Nonnull Builder addResultVars(@Nonnull Collection<String> names) {
            if (resultVars == null) resultVars = new HashSet<>();
            resultVars.addAll(names);
            return this;
        }
        @Contract("_ -> this") @CanIgnoreReturnValue
        public @Nonnull Builder setResultVars(@Nonnull Collection<String> names) {
            resultVars = names instanceof Set ? (Set<String>) names : ImmutableSet.copyOf(names);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder setCardinality(@Nonnull Cardinality cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        public JoinNode build() {
            JoinInfo info = JoinInfo.getPlainJoinability(left, right, joinVars);
            checkArgument(info.isValid(), "Nodes cannot be joined! joinVars="+joinVars);
            Set<String> allVars = union(left.getResultVars(), right.getResultVars());
            if (resultVars != null) {
                Set<String> m = CollectionUtils.setMinus(resultVars, allVars);
                checkArgument(m.isEmpty(), "Some of the selected resultVars are missing: " + m);
            } else {
                resultVars = allVars;
            }
            boolean projecting = resultVars.size() != allVars.size();
            if (cardinality == null)
                cardinality = getCardinality(left, right);
            return new JoinNode(left, right, info.getJoinVars(), resultVars, projecting,
                                info.getPendingRequiredInputs(), info.getPendingOptionalInputs(),
                                cardinality);
        }
    }

    public static @Nonnull Builder builder(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return new Builder(left, right);
    }

    protected JoinNode(@Nonnull PlanNode left, @Nonnull PlanNode right,
                       @Nonnull Set<String> joinVars,
                       @Nonnull Set<String> resultVars, boolean projecting,
                       @Nonnull Set<String> reqInputVars,
                       @Nonnull Set<String> optInputVars,
                       @Nonnull Cardinality cardinality) {
        super(asList(left, right), cardinality, projecting ? resultVars : null,
                !reqInputVars.isEmpty() || !optInputVars.isEmpty());
        this.joinVars = ImmutableSet.copyOf(joinVars);
        this.resultVarsCache = ImmutableSet.copyOf(resultVars);
        this.reqInputs = ImmutableSet.copyOf(reqInputVars);
        this.optInputs = ImmutableSet.copyOf(optInputVars);
    }

    public @Nonnull Set<String> getJoinVars() {
        return joinVars;
    }

    public @Nonnull PlanNode getLeft() {
        return getChildren().get(0);
    }

    public @Nonnull PlanNode getRight() {
        return getChildren().get(1);
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        return reqInputs;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        return optInputs;
    }

    @Override
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        PlanNode left = getLeft().createBound(solution);
        PlanNode right = getRight().createBound(solution);

        Set<String> results = CollectionUtils.setMinus(getResultVars(), solution.getVarNames());
        Set<String> joinVars = CollectionUtils.setMinus(getJoinVars(), solution.getVarNames());
        JoinNode bound = builder(left, right).setJoinVars(joinVars).setResultVars(results).build();
        bound.addBoundFiltersFrom(getFilters(), solution);
        return bound;
    }

    @Override
    public@Nonnull JoinNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        if (map.isEmpty()) return this;

        PlanNode l = map.getOrDefault(getLeft(), getLeft());
        PlanNode r = map.getOrDefault(getRight(), getRight());

        Set<String> joinVars = CollectionUtils.intersect(l.getPublicVars(), r.getPublicVars(), getJoinVars());
        JoinNode newNode = builder(l, r).addJoinVars(joinVars).build();
        newNode.addApplicableFilters(getFilters());
        return newNode;
    }

    private static @Nonnull Cardinality getCardinality(@Nonnull PlanNode l, @Nonnull PlanNode r) {
        Cardinality lc = l.getCardinality(), rc = r.getCardinality();
        return ThresholdCardinalityComparator.DEFAULT.compare(lc, rc) < 0 ? lc : rc;
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        getRight().toString(getLeft().toString(builder).append(" ⋈ "));
        if (isProjecting())
            builder.append(')');
        return builder;
    }

    @Override
    public  @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                               @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        builder.append("⋈{").append(String.join(", ", getJoinVars()))
                .append("} ").append(getCardinality())
                .append(isProjecting() ? ") " : "")
                .append(getVarNamesString()).append(' ').append(getName()).append('\n');
        printFilters(builder, indent2);
        getLeft().prettyPrint(builder, indent2).append('\n');
        getRight().prettyPrint(builder, indent2);
        return builder;
    }
}
