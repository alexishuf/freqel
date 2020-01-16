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
        private boolean intersectInputs = false;

        public @Nonnull Builder add(@Nonnull PlanNode node) {
            list.add(node);
            return this;
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

        public @Nonnull PlanNode buildIfMulti() {
            if (list.size() > 1) return build();
            else return list.get(0);
        }

        public @Nonnull MultiQueryNode build() {
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
        for (PlanNode child : getChildren())
            list.add(map.getOrDefault(child, child));
        return (MultiQueryNode) with(list);
    }

    private @Nonnull PlanNode  with(@Nonnull List<PlanNode> list) {
        if (list.size() == 1)
            return list.get(0);
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
}
