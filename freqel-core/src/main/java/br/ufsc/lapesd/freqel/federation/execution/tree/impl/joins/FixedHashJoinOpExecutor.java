package br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsList;

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
    protected  @Nonnull Results innerExecute(@Nonnull JoinOp node) {
        PlanExecutor exec = getPlanExecutor();
        try (ResultsList<Results> list = new ResultsList<>()) {
            for (Op child : node.getChildren()) list.add(exec.executeNode(child));

            Results results = factory.createResults(list.get(0), list.get(1),
                                                    node.getJoinVars(), node.getResultVars());
            list.clear(); // ownership transferred
            return results;
        }
    }
}
