package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.UPPER_BOUND;

public class SimpleJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    private @Nonnull final HashJoinNodeExecutor hashExecutor;
    private @Nonnull final FixedBindJoinNodeExecutor bindExecutor;
    private @Nonnull final CardinalityComparator comparator;

    @Inject
    public SimpleJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                  @Nonnull BindJoinResultsFactory bindJoinResultsFactory,
                                  @Nonnull CardinalityComparator cardinalityComparator) {
        super(planExecutorProvider);
        this.comparator = cardinalityComparator;
        this.hashExecutor = new HashJoinNodeExecutor(planExecutorProvider, comparator);
        this.bindExecutor = new FixedBindJoinNodeExecutor(planExecutorProvider,
                                                          bindJoinResultsFactory);
    }

    public SimpleJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                  @Nonnull BindJoinResultsFactory bindJoinResultsFactory) {
        super(planExecutor);
        this.comparator = ThresholdCardinalityComparator.DEFAULT;
        this.hashExecutor = new HashJoinNodeExecutor(planExecutor, this.comparator);
        this.bindExecutor = new FixedBindJoinNodeExecutor(planExecutor, bindJoinResultsFactory);

    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        if (node.getLeft().hasInputs() || node.getRight().hasInputs()) {
            return bindExecutor.execute(node);
        } else {
            Cardinality lc = node.getLeft().getCardinality(), rc = node.getRight().getCardinality();
            Cardinality.Reliability lr = lc.getReliability(), rr = rc.getReliability();
            if (lr.isAtLeast(UPPER_BOUND) && rr.isAtLeast(UPPER_BOUND)) {
                if (comparator.min(lc, rc).getValue(Integer.MAX_VALUE) < 1024)
                    return hashExecutor.execute(node);
            }
            return bindExecutor.execute(node);
        }
    }
}
