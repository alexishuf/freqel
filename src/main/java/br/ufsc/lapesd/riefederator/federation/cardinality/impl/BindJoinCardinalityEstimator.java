package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.JoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * A {@link JoinCardinalityEstimator} that simulates the effect of a bidn join to re-estimate
 * the cardinality of the right operand before estimating the join cardinality.
 *
 * Some heuristics are also applied when estiamting the join cardinality.
 */
public class BindJoinCardinalityEstimator implements JoinCardinalityEstimator {
    private final @Nonnull CardinalityEnsemble cardinalityEnsemble;

    @Inject
    public BindJoinCardinalityEstimator(@Nonnull CardinalityEnsemble cardinalityEnsemble) {
        this.cardinalityEnsemble = cardinalityEnsemble;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull JoinInfo i) {
        CQuery union = CQuery.union(getTriples(i.getLeft()), getTriples(i.getRight()));
        return cardinalityEnsemble.estimate(union, null);
    }

    private @Nonnull Collection<Triple> getTriples(@Nonnull PlanNode node) {
        if (node instanceof QueryNode)
            return ((QueryNode) node).getQuery();
        if (node instanceof MultiQueryNode) {
            LinkedHashSet<Triple> set = new LinkedHashSet<>();
            node.getChildren().stream().flatMap(n -> getTriples(n).stream()).forEach(set::add);
            return set;
        }
        return node.getMatchedTriples();
    }
}
