package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class FixedHashJoinOpExecutor extends AbstractSimpleJoinOpExecutor {
    private final @Nonnull HashJoinResultsFactory factory;

    @Inject
    public FixedHashJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                   @Nonnull HashJoinResultsFactory factory) {
        super(planExecutorProvider);
        this.factory = factory;
    }

    public FixedHashJoinOpExecutor(@Nonnull PlanExecutor planExecutor,
                                   @Nonnull HashJoinResultsFactory factory) {
        super(planExecutor);
        this.factory = factory;
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinOp node) {
        PlanExecutor exec = getPlanExecutor();
        try (ResultsList<Results> list = new ResultsList<>()) {
            for (Op child : node.getChildren()) list.add(exec.executeNode(child));

            Results results = factory.createResults(list.get(0), list.get(1),
                                                    node.getJoinVars(), node.getResultVars());
            list.clear(); // ownership transferred
            return SPARQLFilterResults.applyIf(results, node);
        }
    }
}
