package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.DefaultJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedBindJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.BufferedResultsExecutor;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.AbstractModule;

import javax.annotation.Nonnull;

public class SimpleExecutionModule extends AbstractModule {
    protected boolean allowHashJoins = true;

    @CanIgnoreReturnValue
    public @Nonnull SimpleExecutionModule forceBindJoins() {
        allowHashJoins = false;
        return this;
    }

    @Override
    protected void configure() {
        configureResultsExecutor();
        bind(QueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
        bind(MultiQueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
        bind(CartesianNodeExecutor.class).to(LazyCartesianNodeExecutor.class);
        bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
        if (allowHashJoins)
            bind(JoinNodeExecutor.class).to(DefaultJoinNodeExecutor.class);
        else
            bind(JoinNodeExecutor.class).to(FixedBindJoinNodeExecutor.class);
        bind(EmptyNodeExecutor.class).toInstance(SimpleEmptyNodeExecutor.INSTANCE);
        bind(PlanExecutor.class).to(InjectedExecutor.class);
    }

    protected void configureResultsExecutor() {
        bind(ResultsExecutor.class).toInstance(new BufferedResultsExecutor());
    }
}
