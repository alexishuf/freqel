package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.setMinus;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.union;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * This node represents multiple independent queries that output the same projection.
 *
 * There is no need to perform duplicate removals at this stage (hence it is not a Union).
 */
public class MultiQueryNode extends AbstractInnerPlanNode {
    private final boolean intersectInputs;

    public static class Builder {
        private List<PlanNode> list = new ArrayList<>();
        private Set<String> resultVars = null;
        private boolean intersect = false;
        private boolean intersectInputs = true;
        private Cardinality cardinality = null;

        @CanIgnoreReturnValue
        public @Nonnull Builder add(@Nonnull PlanNode node) {
            list.add(node);
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder addAll(@Nonnull PlanNode maybeMultiQueryNode) {
            if (maybeMultiQueryNode instanceof MultiQueryNode)
                return addAll(maybeMultiQueryNode.getChildren());
            else
                return add(maybeMultiQueryNode);
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAll(@Nonnull Collection<? extends PlanNode> collection) {
            collection.forEach(this::add);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder setResultVars(@Nonnull Collection<String> varNames) {
            resultVars = varNames instanceof Set? (Set<String>)varNames : new HashSet<>(varNames);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder intersect() {
            intersect = true;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder allVars() {
            intersect = false;
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder intersectInputs() {
            intersectInputs = true;
            return this;
        }
        @CanIgnoreReturnValue
        public @Nonnull Builder allInputs() {
            intersectInputs = false;
            return this;
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder withCardinality(@Nonnull Cardinality cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        @CheckReturnValue
        public @Nonnull PlanNode buildIfMulti() {
            Preconditions.checkState(!isEmpty(), "Builder is empty");
            if (list.size() > 1) return build();
            else return list.get(0);
        }

        @CheckReturnValue
        public @Nonnull MultiQueryNode build() {
            Preconditions.checkState(!isEmpty(), "Builder is empty");
            Set<String> all = TreeUtils.union(list, PlanNode::getResultVars);
            if (resultVars == null) {
                if (intersect)
                    resultVars = TreeUtils.intersect(list, PlanNode::getResultVars);
                else
                    resultVars = all;
            } else {
                checkArgument(all.containsAll(resultVars), "There are extra result vars");
            }
            if (cardinality == null) {
                cardinality = list.stream().map(PlanNode::getCardinality)
                        .min(ThresholdCardinalityComparator.DEFAULT).orElse(Cardinality.UNSUPPORTED);
            }
            return new MultiQueryNode(list, ImmutableSet.copyOf(resultVars),
                            resultVars.size() != all.size(),
                                      intersectInputs, cardinality);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    protected MultiQueryNode(@Nonnull List<PlanNode> children,
                             @Nonnull ImmutableSet<String> resultVars,
                             boolean projecting,
                             boolean intersectInputs,
                             @Nonnull Cardinality cardinality) {
        super(children, cardinality, projecting ? resultVars : null);
        this.intersectInputs = intersectInputs;
        this.resultVarsCache = resultVars;
    }

    @Override
    public @Nonnull Set<String> getInputVars() {
        if (!intersectInputs)
            return super.getInputVars();
        if (allInputVarsCache == null)
            allInputVarsCache = TreeUtils.intersect(getChildren(), PlanNode::getInputVars);
        return allInputVarsCache;
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        if (!intersectInputs)
            return super.getRequiredInputVars();
        if (reqInputsCache == null)
            reqInputsCache = TreeUtils.intersect(super.getRequiredInputVars(), getInputVars());
        return reqInputsCache;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        if (!intersectInputs)
            return super.getOptionalInputVars();
        if (optInputsCache == null)
            optInputsCache = TreeUtils.intersect(getInputVars(), super.getOptionalInputVars());
        return optInputsCache;
    }

    @Override
    public @Nonnull MultiQueryNode createBound(@Nonnull Solution solution) {
        Builder builder = builder();
        getChildren().stream().map(child -> child.createBound(solution)).forEach(builder::add);
        if (intersectInputs) builder.intersectInputs();
        builder.setResultVars(setMinus(getResultVars(), solution.getVarNames()));
        MultiQueryNode bound = builder.build();
        bound.addBoundFiltersFrom(getFilters(), solution);
        return bound;
    }

    @Override
    public @Nonnull MultiQueryNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        checkArgument(union(getChildren(), PlanNode::getResultVars).containsAll(getResultVars()),
                      "Replacement removes resultVariables of the MultiQueryNode");

        Builder builder = builder();
        boolean change = false;
        for (PlanNode child : getChildren()) {
            PlanNode replacement = map.getOrDefault(child, null);
            change |= replacement != null;
            builder.add(replacement == null ? child : replacement);
        }
        if (!change)
            return this;
        if (intersectInputs) builder.intersectInputs();
        builder.setResultVars(getResultVars());
        MultiQueryNode newNode = builder.build();
        newNode.addApplicableFilters(getFilters());
        return newNode;
    }

    public @Nonnull PlanNode  with(@Nonnull List<PlanNode> list) {
        if (list.size() == 1)
            return list.get(0);
        if (list.equals(getChildren()))
            return this;
        Builder builder = builder();
        builder.addAll(list);
        if (intersectInputs) builder.intersectInputs();
        builder.setResultVars(getResultVars());
        return builder.build();
    }

    public @Nullable PlanNode without(@Nonnull PlanNode node) {
        return without(Collections.singleton(node));
    }

    public @Nullable PlanNode without(@Nonnull Collection<PlanNode> nodes) {
        if (nodes.isEmpty()) return this;

        Builder builder = builder();
        getChildren().stream().filter(n -> !nodes.contains(n)).forEach(builder::add);
        if (builder.isEmpty())
            return null; // no children
        if (builder.list.size() == getChildren().size())
            return this; // no change
        if (intersectInputs)
            builder.intersectInputs();
        checkArgument(union(builder.list, PlanNode::getResultVars).containsAll(getResultVars()),
                      "without("+nodes+") removes some of the result variables");
        builder.setResultVars(getResultVars());
        return builder.buildIfMulti();
    }

    @Override
    public  @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
        if (isProjecting())
            b.append(getPiWithNames()).append('(');
        for (PlanNode child : getChildren())
            child.toString(b).append(" + ");
        b.setLength(b.length()-3);
        if (isProjecting())
            b.append(')');
        return b;
    }

    @Override
    public  @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                                 @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        builder.append(getChildren().isEmpty() ? "Empty" : "")
                .append("Multi-node").append(isProjecting() ? ")" : getVarNamesString())
                .append(' ').append(getCardinality());
        if (getChildren().isEmpty())
            return builder;
        else
            builder.append('\n');
        printFilters(builder, indent2);
        for (PlanNode child : getChildren())
            child.prettyPrint(builder, indent2).append('\n');
        builder.setLength(builder.length()-1);
        return builder;
    }
}
