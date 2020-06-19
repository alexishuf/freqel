package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.LazyCartesianResults;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Set;

public class LazyCartesianNodeExecutor extends SimpleNodeExecutor implements CartesianNodeExecutor {

    @Inject
    public LazyCartesianNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public LazyCartesianNodeExecutor(@Nonnull PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public @Nonnull Results execute(@Nonnull CartesianNode node) {
        PlanExecutor planExecutor = getPlanExecutor();
        try (ResultsList<Results> list = new ResultsList<>()) {
            for (PlanNode child : node.getChildren())
                list.add(planExecutor.executeNode(child));
            Set<String> varNames = node.getResultVars();
            // parallelizing the inputs provides no significant improvement
            // the parallelization provided by lazyness is enough and is significant
            return new LazyCartesianResults(list.steal(), varNames);
        }
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return CartesianNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof CartesianNode);
        return execute((CartesianNode)node);
    }
}
