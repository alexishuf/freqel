package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class FixedBindJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
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
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        PlanExecutor planExecutor = getPlanExecutor();
        Results leftResults = null;
        try {
            leftResults = planExecutor.executeNode(node.getLeft());
            Results results = factory.createResults(leftResults, node.getRight(),
                                                    node.getJoinVars(), node.getResultVars());
            leftResults = null;
            return results;
        } finally {
            if (leftResults != null)
                leftResults.close();
        }
    }
}
