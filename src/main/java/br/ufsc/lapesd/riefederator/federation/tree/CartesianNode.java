package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.CardinalityComparator;
import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.unionInputs;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.unionResults;
import static java.util.stream.Collectors.toList;

public class CartesianNode extends AbstractPlanNode {
    public CartesianNode(@Nonnull List<PlanNode> children) {
        this(children, computeCardinality(children));
    }

    private static @Nonnull Cardinality computeCardinality(@Nonnull Collection<PlanNode> children) {
        Cardinality max = children.stream().map(PlanNode::getCardinality)
                .max(CardinalityComparator.DEFAULT).orElse(Cardinality.UNSUPPORTED);
        if (max.getReliability().ordinal() >= Cardinality.Reliability.LOWER_BOUND.ordinal()) {
            int value = max.getValue(Integer.MAX_VALUE);
            assert value != Integer.MAX_VALUE;
            double pow = Math.pow(value, children.size());
            return new Cardinality(max.getReliability(), (int)Math.min(Integer.MAX_VALUE, pow));
        }
        return max;
    }

    public CartesianNode(@Nonnull List<PlanNode> children, @Nonnull Cardinality cardinality) {
        super(unionResults(children), unionResults(children), false, unionInputs(children),
              children, cardinality);
    }

    @Override
    public @Nonnull
    PlanNode createBound(@Nonnull Solution solution) {
        return new CartesianNode(getChildren().stream()
                .map(n -> n.createBound(solution)).collect(toList()));
    }

    @Override
    public @Nonnull CartesianNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        if (map.isEmpty()) return this;
        Preconditions.checkArgument(getChildren().containsAll(map.keySet()));
        List<PlanNode> list = new ArrayList<>(getChildren().size());
        for (PlanNode child : getChildren()) list.add(map.getOrDefault(child, child));
        return new CartesianNode(list);
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
        if (isProjecting())
            b.append(getPiWithNames()).append('(');
        for (PlanNode child : getChildren())
            child.toString(b).append(" × ");
        b.setLength(b.length()-3);
        if (isProjecting())
            b.append(')');
        return b;
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                                 @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjecting())
            builder.append(getPiWithNames()).append('(');
        builder.append(getChildren().isEmpty() ? "Empty" : "").append('×')
                .append(isProjecting() ? ")" : getVarNamesString())
                .append(' ').append(getCardinality());
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
