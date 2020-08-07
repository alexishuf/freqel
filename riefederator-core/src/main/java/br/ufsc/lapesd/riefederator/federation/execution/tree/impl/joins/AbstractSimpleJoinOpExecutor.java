package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleOpExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Provider;

public abstract class AbstractSimpleJoinOpExecutor extends SimpleOpExecutor
                                                  implements JoinOpExecutor {
    public AbstractSimpleJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public AbstractSimpleJoinOpExecutor(@Nonnull PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends Op> nodeClass) {
        return JoinOp.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull Op node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof JoinOp, "Only JoinNode instance allowed");
        return execute((JoinOp)node);
    }
}
