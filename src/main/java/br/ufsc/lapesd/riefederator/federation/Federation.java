package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.*;
import br.ufsc.lapesd.riefederator.query.impl.HashDistinctResults;
import br.ufsc.lapesd.riefederator.query.impl.ProjectingResults;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.inject.Inject;


/**
 * A {@link CQEndpoint} that decomposes queries into the registered {@link Source}s.
 */
public class Federation extends AbstractTPEndpoint implements CQEndpoint {
    private final @Nonnull DecompositionStrategy strategy;
    private final @Nonnull PlanExecutor executor;

    @Inject
    public Federation(@Nonnull DecompositionStrategy strategy,
                      @Nonnull PlanExecutor executor) {
        this.strategy = strategy;
        this.executor = executor;
    }

    public @Nonnull DecompositionStrategy getDecompositionStrategy() {
        return strategy;
    }

    public static @Nonnull Federation createDefault() {
        Injector injector = Guice.createInjector(new SimpleFederationModule());
        return injector.getInstance(Federation.class);
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull Federation addSource(@Nonnull Source source) {
        strategy.addSource(source);
        return this;
    }

    public @Nonnull PlanNode plan(@Nonnull CQuery query) {
        ModifierUtils.check(this, query.getModifiers());
        return strategy.decompose(query);
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        Results results = executor.executePlan(plan(query));
        results = ProjectingResults.applyIf(results, query);
        results = HashDistinctResults.applyIf(results, query);
        return results;
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability){
            case DISTINCT:
            case PROJECTION:
                return true;
            default:
                return false;
        }
    }
}
