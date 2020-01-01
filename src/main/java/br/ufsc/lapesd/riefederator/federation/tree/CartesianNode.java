package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.query.Solution;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class CartesianNode extends PlanNode {
    public CartesianNode(@Nonnull List<PlanNode> children) {
        super(computeVars(children), false, children);
    }

    private static @Nonnull Set<String> computeVars(@Nonnull List<PlanNode> children) {
        Set<String> set = new HashSet<>();
        for (PlanNode child : children)
            set.addAll(child.getResultVars());
        return set;
    }

    @Override
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        return new CartesianNode(getChildren().stream()
                .map(n -> n.createBound(solution)).collect(toList()));
    }

    @Override
    protected @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
        if (isProjecting())
            b.append(getPiWithNames()).append('(');
        for (PlanNode child : getChildren())
            child.toString(b).append(" × ");
        b.setLength(b.length()-3);
        if (isProjecting())
            b.append(')');
        return b;
    }
}
