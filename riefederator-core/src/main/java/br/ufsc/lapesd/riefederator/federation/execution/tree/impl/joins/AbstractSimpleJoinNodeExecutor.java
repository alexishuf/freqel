package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.results.Results;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Provider;

public abstract class AbstractSimpleJoinNodeExecutor extends SimpleNodeExecutor
                                                  implements JoinNodeExecutor {
    public AbstractSimpleJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public AbstractSimpleJoinNodeExecutor(@Nonnull PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return JoinNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof JoinNode, "Only JoinNode instance allowed");
        return execute((JoinNode)node);
    }
}
