package br.ufsc.lapesd.freqel.federation.execution.tree.impl;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.PipeOpExecutor;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsUtils;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

public class SimplePipeOpExecutor extends SimpleOpExecutor implements PipeOpExecutor {
    @Inject
    protected SimplePipeOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    @Override public boolean canExecute(@Nonnull Class<? extends Op> nodeClass) {
        return PipeOp.class.isAssignableFrom(nodeClass);
    }

    @Override public @Nonnull Results execute(@Nonnull PipeOp op) {
        assert op.getChildren().size() == 1;
        Op child = op.getChildren().get(0);
        Results r = getPlanExecutor().executeNode(child);
        return ResultsUtils.applyModifiers(r, op.modifiers());
    }

    @Override public @Nonnull Results execute(@Nonnull Op node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof PipeOp);
        return execute((PipeOp)node);
    }
}
