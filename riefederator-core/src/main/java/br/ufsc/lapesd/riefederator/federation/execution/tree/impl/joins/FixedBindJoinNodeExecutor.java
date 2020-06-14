package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class FixedBindJoinNodeExecutor extends AbstractBindJoinNodeExecutor {
    private final @Nonnull BindJoinResultsFactory factory;

    @Inject
    public FixedBindJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                     @Nonnull BindJoinResultsFactory factory) {
        super(planExecutorProvider);
        this.factory = factory;
    }

    public FixedBindJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                     @Nonnull BindJoinResultsFactory factory) {
        super(planExecutor);
        this.factory = factory;
    }

    @Override
    protected @Nonnull Results createResults(@Nonnull Results left, @Nonnull PlanNode right,
                                             @Nonnull JoinNode node) {
        return factory.createResults(left, right, node.getJoinVars(), node.getResultVars());
    }

}
