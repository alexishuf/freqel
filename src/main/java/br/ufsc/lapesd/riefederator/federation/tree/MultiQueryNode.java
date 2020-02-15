package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * This node represents multiple independent queries that output the same projection.
 *
 * There is no need to perform duplicate removals at this stage (hence it is not a Union).
 */
public class MultiQueryNode extends PlanNode {
    public static class Builder {
        private List<PlanNode> list = new ArrayList<>();
        private Set<String> resultVars = null;
        private boolean project = false;
        private boolean intersect = false;
        private boolean intersectInputs = true;

        public @Nonnull Builder add(@Nonnull PlanNode node) {
            list.add(node);
            return this;
        }
        public @Nonnull Builder addAll(@Nonnull PlanNode maybeMultiQueryNode) {
            if (maybeMultiQueryNode instanceof MultiQueryNode)
                return addAll(maybeMultiQueryNode.getChildren());
            else
                return add(maybeMultiQueryNode);
        }

        public @Nonnull Builder addAll(@Nonnull Collection<? extends PlanNode> collection) {
            collection.forEach(this::add);
            return this;
        }

        public @Nonnull Builder setResultVarsNoProjection(@Nonnull Collection<String> varNames) {
            resultVars = varNames instanceof Set? (Set<String>)varNames : new HashSet<>(varNames);
            project = false;
            return this;
        }

        public @Nonnull Builder intersect() {
            intersect = true;
            project = true;
            return this;
        }
        public @Nonnull Builder allVars() {
            intersect = false;
            project = false;
            return this;
        }

        public @Nonnull Builder intersectInputs() {
            intersectInputs = true;
            return this;
        }
        public @Nonnull Builder allInputs() {
            intersectInputs = false;
            return this;
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        public @Nonnull PlanNode buildIfMulti() {
            Preconditions.checkState(!isEmpty(), "Builder is empty");
            if (list.size() > 1) return build();
            else return list.get(0);
        }

        public @Nonnull MultiQueryNode build() {
            Preconditions.checkState(!isEmpty(), "Builder is empty");
            if (resultVars == null) {
                if (intersect) {
                    resultVars = intersectResults(list);
                } else {
                    project = false;
                    resultVars = unionResults(list);
                }
            } else if (MultiQueryNode.class.desiredAssertionStatus()) {
                Set<String> all = unionResults(list);
                Preconditions.checkArgument(all.containsAll(resultVars),
                        "There are extra result vars");
                Preconditions.checkArgument(intersect || project == !resultVars.equals(all),
                        "Mismatch between project and resultVars");
            }
            Set<String> inputs = intersectInputs ? TreeUtils.intersectInputs(list)
                                                 : TreeUtils.unionInputs(list);
            return new MultiQueryNode(list, resultVars, project, inputs);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder();
    }

    protected MultiQueryNode(@Nonnull List<PlanNode> children,
                             @Nonnull Collection<String> resultVars,
                             boolean projecting,
                             @Nonnull Collection<String> inputVars) {
        super(resultVars, projecting, inputVars, children);
    }

    @Override
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        List<PlanNode> children = getChildren().stream().map(n -> n.createBound(solution))
                                                        .collect(toList());
        Set<String> all = children.stream().flatMap(n -> getResultVars().stream()).collect(toSet());
        HashSet<String> names = new HashSet<>(getResultVars());
        names.retainAll(all);
        boolean projecting = names.size() < all.size();
        HashSet<String> inputs = new HashSet<>(getInputVars());
        inputs.removeAll(solution.getVarNames());
        return new MultiQueryNode(children, names, projecting, inputs);
    }

    @Override
    public @Nonnull MultiQueryNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        Preconditions.checkArgument(getChildren().containsAll(map.keySet()));
        if (map.isEmpty()) return this;

        List<PlanNode> list = new ArrayList<>();
        for (PlanNode child : getChildren()) {
            PlanNode replacement = map.getOrDefault(child, child);
            if (replacement != null)
                list.add(replacement);
        }
        return (MultiQueryNode) with(list);
    }

    public @Nonnull PlanNode  with(@Nonnull List<PlanNode> list) {
        if (list.size() == 1)
            return list.get(0);
        if (list.equals(getChildren()))
            return this;
        Set<String> allResults = unionResults(list);
        Set<String> results = intersect(getResultVars(), allResults);
        Set<String> inputs  = intersect(getInputVars(),  unionInputs(list));
        boolean projecting = results.size() != allResults.size();
        return new MultiQueryNode(list, results, projecting, inputs);
    }

    public @Nullable PlanNode without(@Nonnull PlanNode node) {
        if (!getChildren().contains(node))
            return this;
        List<PlanNode> left = new ArrayList<>(getChildren().size());
        for (PlanNode child : getChildren()) {
            if (!node.equals(child)) left.add(child);
        }
        return left.isEmpty() ? null : with(left);
    }

    public @Nullable PlanNode without(@Nonnull Collection<PlanNode> nodes) {
        if (nodes.isEmpty()) return this;
        int size = getChildren().size();
        List<PlanNode> list = new ArrayList<>(size);
        for (PlanNode child : getChildren()) {
            if (!nodes.contains(child)) list.add(child);
        }
        if (list.isEmpty())      return null;
        if (list.size() == size) return this;
        else                     return with(list);
    }

    @Override
    protected @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
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
    protected @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                                 @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        builder.append(getChildren().isEmpty() ? "Empty" : "")
                .append("Multi-node").append(isProjecting() ? ")" : getVarNamesString());
        if (getChildren().isEmpty())
            return builder;
        else
            builder.append('\n');
        for (PlanNode child : getChildren())
            child.prettyPrint(builder, indent2).append('\n');
        builder.setLength(builder.length()-1);
        return builder;
    }
}
