package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleOpExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Provider;

public abstract class AbstractSimpleJoinOpExecutor extends SimpleOpExecutor
                                                  implements JoinOpExecutor {
    protected AbstractSimpleJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    protected AbstractSimpleJoinOpExecutor(@Nonnull PlanExecutor planExecutor) {
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

    protected abstract @Nonnull Results innerExecute(@Nonnull JoinOp node);

    public final @Nonnull Results execute(@Nonnull JoinOp node) {
        return ResultsUtils.applyModifiers(innerExecute(node), node.modifiers());
    }
}
