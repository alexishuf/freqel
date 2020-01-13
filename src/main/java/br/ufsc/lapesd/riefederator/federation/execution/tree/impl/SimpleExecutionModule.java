package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.SimpleJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import com.google.inject.AbstractModule;

public class SimpleExecutionModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(QueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
        bind(MultiQueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
        bind(CartesianNodeExecutor.class).to(SimpleCartesianNodeExecutor.class);
        bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
        bind(JoinNodeExecutor.class).to(SimpleJoinNodeExecutor.class);
        bind(EmptyNodeExecutor.class).toInstance(SimpleEmptyNodeExecutor.INSTANCE);
        bind(PlanExecutor.class).to(InjectedExecutor.class);
    }
}
