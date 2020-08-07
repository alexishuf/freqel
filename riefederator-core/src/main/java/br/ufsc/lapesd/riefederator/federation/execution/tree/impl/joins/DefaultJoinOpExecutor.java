package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

import static br.ufsc.lapesd.riefederator.algebra.Cardinality.Reliability.UPPER_BOUND;
import static br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityUtils.multiply;

public class DefaultJoinOpExecutor extends AbstractSimpleJoinOpExecutor {
    private @Nonnull final DefaultHashJoinOpExecutor hashExecutor;
    private @Nonnull final FixedBindJoinOpExecutor bindExecutor;
    private @Nonnull final CardinalityComparator comparator;

    @Inject
    public DefaultJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                 @Nonnull BindJoinResultsFactory bindJoinResultsFactory,
                                 @Nonnull CardinalityComparator cardinalityComparator) {
        super(planExecutorProvider);
        this.comparator = cardinalityComparator;
        this.hashExecutor = new DefaultHashJoinOpExecutor(planExecutorProvider, comparator);
        this.bindExecutor = new FixedBindJoinOpExecutor(planExecutorProvider,
                                                          bindJoinResultsFactory);
    }

    public DefaultJoinOpExecutor(@Nonnull PlanExecutor planExecutor,
                                 @Nonnull BindJoinResultsFactory bindJoinResultsFactory) {
        super(planExecutor);
        this.comparator = ThresholdCardinalityComparator.DEFAULT;
        this.hashExecutor = new DefaultHashJoinOpExecutor(planExecutor, this.comparator);
        this.bindExecutor = new FixedBindJoinOpExecutor(planExecutor, bindJoinResultsFactory);
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinOp node) {
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

            Op m = comparator.compare(rc, lc) >= 0 ? node.getRight() : node.getLeft();
            boolean askDegenerate = node.getJoinVars().containsAll(m.getPublicVars());
            Cardinality askDegenCeil = comparator.min(Cardinality.guess(2000),
                                                      multiply(minC, 2));
            if (askDegenerate && comparator.compare(m.getCardinality(), askDegenCeil) <= 0)
                return SPARQLFilterResults.applyIf(hashExecutor.execute(node), node);

            return SPARQLFilterResults.applyIf(bindExecutor.execute(node), node);
        }
    }
}
