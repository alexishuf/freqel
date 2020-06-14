package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.NodeExecutor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.Objects;

public abstract class SimpleNodeExecutor implements NodeExecutor {
    private @Nullable PlanExecutor planExecutor;
    private final @Nullable Provider<PlanExecutor> planExecutorProvider;

    public SimpleNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        this.planExecutorProvider = planExecutorProvider;
        this.planExecutor = null;
    }

    public SimpleNodeExecutor(@Nonnull PlanExecutor planExecutor) {
        this.planExecutorProvider = null;
        this.planExecutor = planExecutor;
    }

    protected @Nonnull PlanExecutor getPlanExecutor() {
        if (planExecutor != null) return planExecutor;
        return Objects.requireNonNull(planExecutorProvider).get();
    }

    public void setPlanExecutor(@Nonnull PlanExecutor planExecutor) {
        this.planExecutor = planExecutor;
    }
}
