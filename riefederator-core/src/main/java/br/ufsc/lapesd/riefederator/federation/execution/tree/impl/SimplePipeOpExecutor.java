package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.PipeOpExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.AskResults;
import br.ufsc.lapesd.riefederator.query.results.impl.LimitResults;
import br.ufsc.lapesd.riefederator.query.results.impl.ProjectingResults;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
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
        if (op.modifiers().optional() != null)
            r.setOptional(true);
        //else: get optionality from child

        r = SPARQLFilterResults.applyIf(r, op);
        r = LimitResults.applyIf(r, op.modifiers());
        r = ProjectingResults.applyIf(r, op.modifiers());
        r = AskResults.applyIf(r, op.modifiers());
        return r;
    }

    @Override public @Nonnull Results execute(@Nonnull Op node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof PipeOp);
        return execute((PipeOp)node);
    }
}
