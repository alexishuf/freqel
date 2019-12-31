package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.MultiQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.QueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.HashJoinNodeExecutor;
import com.google.inject.AbstractModule;

public class SimpleExecutionModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(QueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
        bind(MultiQueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
        bind(CartesianNodeExecutor.class).to(SimpleCartesianNodeExecutor.class);
        bind(JoinNodeExecutor.class).to(HashJoinNodeExecutor.class);
        bind(PlanExecutor.class).to(InjectedExecutor.class);
    }
}
