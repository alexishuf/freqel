package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.query.Solution;

import javax.annotation.Nonnull;
import java.util.List;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.unionInputs;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.unionResults;
import static java.util.stream.Collectors.toList;

public class CartesianNode extends PlanNode {
    public CartesianNode(@Nonnull List<PlanNode> children) {
        super(unionResults(children), false, unionInputs(children), children);
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
