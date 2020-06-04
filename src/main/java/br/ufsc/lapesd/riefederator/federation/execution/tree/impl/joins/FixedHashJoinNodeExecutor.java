package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class FixedHashJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    private final @Nonnull HashJoinResultsFactory factory;

    @Inject
    public FixedHashJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                     @Nonnull HashJoinResultsFactory factory) {
        super(planExecutorProvider);
        this.factory = factory;
    }

    public FixedHashJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                     @Nonnull HashJoinResultsFactory factory) {
        super(planExecutor);
        this.factory = factory;
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        PlanExecutor exec = getPlanExecutor();
        try (ResultsList list = new ResultsList()) {
            for (PlanNode child : node.getChildren()) list.add(exec.executeNode(child));

            Results results = factory.createResults(list.get(0), list.get(1),
                                                    node.getJoinVars(), node.getResultVars());
            list.clear(); // ownership transferred
            return SPARQLFilterResults.applyIf(results, node);
        }
    }
}
