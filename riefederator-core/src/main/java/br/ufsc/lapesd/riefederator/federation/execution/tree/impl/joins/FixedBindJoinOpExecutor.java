package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class FixedBindJoinOpExecutor extends AbstractBindJoinOpExecutor {
    private final @Nonnull BindJoinResultsFactory factory;

    @Inject
    public FixedBindJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                   @Nonnull BindJoinResultsFactory factory) {
        super(planExecutorProvider);
        this.factory = factory;
    }

    public FixedBindJoinOpExecutor(@Nonnull PlanExecutor planExecutor,
                                   @Nonnull BindJoinResultsFactory factory) {
        super(planExecutor);
        this.factory = factory;
    }

    @Override
    protected @Nonnull Results createResults(@Nonnull Results left, @Nonnull Op right,
                                             @Nonnull JoinOp node) {
        return factory.createResults(left, right, node.getJoinVars(), node.getResultVars());
    }

}
