package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.DefaultJoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedBindJoinOpExecutor;
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
        bind(QueryOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(MultiQueryOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(CartesianOpExecutor.class).to(LazyCartesianOpExecutor.class);
        bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
        if (allowHashJoins)
            bind(JoinOpExecutor.class).to(DefaultJoinOpExecutor.class);
        else
            bind(JoinOpExecutor.class).to(FixedBindJoinOpExecutor.class);
        bind(EmptyOpExecutor.class).toInstance(SimpleEmptyOpExecutor.INSTANCE);
        bind(SPARQLValuesTemplateOpExecutor.class).to(SimpleQueryOpExecutor.class);
        bind(PlanExecutor.class).to(InjectedExecutor.class);
    }

    protected void configureResultsExecutor() {
        bind(ResultsExecutor.class).toInstance(new BufferedResultsExecutor());
    }
}
