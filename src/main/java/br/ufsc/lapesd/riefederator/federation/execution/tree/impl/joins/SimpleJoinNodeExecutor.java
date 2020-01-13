package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.query.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class SimpleJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    private @Nonnull HashJoinNodeExecutor hashExecutor;
    private @Nonnull FixedBindJoinNodeExecutor bindExecutor;

    @Inject
    public SimpleJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                  @Nonnull BindJoinResultsFactory bindJoinResultsFactory) {
        super(planExecutorProvider);
        this.hashExecutor = new HashJoinNodeExecutor(planExecutorProvider);
        this.bindExecutor = new FixedBindJoinNodeExecutor(planExecutorProvider,
                                                          bindJoinResultsFactory);
    }

    public SimpleJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                  @Nonnull BindJoinResultsFactory bindJoinResultsFactory) {
        super(planExecutor);
        this.hashExecutor = new HashJoinNodeExecutor(planExecutor);
        this.bindExecutor = new FixedBindJoinNodeExecutor(planExecutor, bindJoinResultsFactory);
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        if (node.getLeft().hasInputs() || node.getRight().hasInputs()) {
            return bindExecutor.execute(node);
        } else {
            return hashExecutor.execute(node);
        }
    }
}
