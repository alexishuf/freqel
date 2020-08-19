package br.ufsc.lapesd.riefederator.federation.cardinality.impl;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.JoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;

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
        CQuery union = CQuery.merge(getTriples(i.getLeft()), getTriples(i.getRight()));
        return cardinalityEnsemble.estimate(union, null);
    }

    private @Nonnull Collection<Triple> getTriples(@Nonnull Op node) {
        if (node instanceof EndpointQueryOp)
            return ((EndpointQueryOp) node).getQuery();
        if (node instanceof UnionOp) {
            LinkedHashSet<Triple> set = new LinkedHashSet<>();
            node.getChildren().stream().flatMap(n -> getTriples(n).stream()).forEach(set::add);
            return set;
        }
        return node.getMatchedTriples();
    }
}
