package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Capability;
import br.ufsc.lapesd.riefederator.query.Results;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.inject.Inject;


/**
 * A {@link CQEndpoint} that decomposes queries into the registered {@link Source}s.
 */
public class Federation implements CQEndpoint {
    private final @Nonnull DecompositionStrategy strategy;
    private final @Nonnull PlanExecutor executor;

    @Inject
    public Federation(@Nonnull DecompositionStrategy strategy,
                      @Nonnull PlanExecutor executor) {
        this.strategy = strategy;
        this.executor = executor;
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull Federation addSource(@Nonnull Source source) {
        strategy.addSource(source);
        return this;
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        PlanNode plan = strategy.decompose(query);
        return executor.executePlan(plan);
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        return false;
    }
}
