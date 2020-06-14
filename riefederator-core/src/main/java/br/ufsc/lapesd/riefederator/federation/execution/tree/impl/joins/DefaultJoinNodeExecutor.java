package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

import static br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityUtils.multiply;
import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.UPPER_BOUND;

public class DefaultJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    private @Nonnull final DefaultHashJoinNodeExecutor hashExecutor;
    private @Nonnull final FixedBindJoinNodeExecutor bindExecutor;
    private @Nonnull final CardinalityComparator comparator;

    @Inject
    public DefaultJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                   @Nonnull BindJoinResultsFactory bindJoinResultsFactory,
                                   @Nonnull CardinalityComparator cardinalityComparator) {
        super(planExecutorProvider);
        this.comparator = cardinalityComparator;
        this.hashExecutor = new DefaultHashJoinNodeExecutor(planExecutorProvider, comparator);
        this.bindExecutor = new FixedBindJoinNodeExecutor(planExecutorProvider,
                                                          bindJoinResultsFactory);
    }

    public DefaultJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                   @Nonnull BindJoinResultsFactory bindJoinResultsFactory) {
        super(planExecutor);
        this.comparator = ThresholdCardinalityComparator.DEFAULT;
        this.hashExecutor = new DefaultHashJoinNodeExecutor(planExecutor, this.comparator);
        this.bindExecutor = new FixedBindJoinNodeExecutor(planExecutor, bindJoinResultsFactory);
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        if (node.getLeft().hasInputs() || node.getRight().hasInputs()) {
            return bindExecutor.execute(node);
        } else {
            Cardinality lc = node.getLeft().getCardinality(), rc = node.getRight().getCardinality();
            int diff = comparator.compare(lc, rc);
            Cardinality minC = diff <= 0 ? lc : rc;
            Cardinality.Reliability lr = lc.getReliability(), rr = rc.getReliability();
            if (lr.isAtLeast(UPPER_BOUND) && rr.isAtLeast(UPPER_BOUND)) {
                if (minC.getValue(Integer.MAX_VALUE) < 1024)
                    return SPARQLFilterResults.applyIf(hashExecutor.execute(node), node);
            }

            PlanNode m = comparator.compare(rc, lc) >= 0 ? node.getRight() : node.getLeft();
            boolean askDegenerate = node.getJoinVars().containsAll(m.getPublicVars());
            Cardinality askDegenCeil = comparator.min(Cardinality.guess(2000),
                                                      multiply(minC, 2));
            if (askDegenerate && comparator.compare(m.getCardinality(), askDegenCeil) <= 0)
                return SPARQLFilterResults.applyIf(hashExecutor.execute(node), node);

            return SPARQLFilterResults.applyIf(bindExecutor.execute(node), node);
        }
    }
}
