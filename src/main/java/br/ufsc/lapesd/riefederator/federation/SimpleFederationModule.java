package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.StandardDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinPathsPlanner;

public class SimpleFederationModule extends SimpleExecutionModule {
    @Override
    protected void configure() {
        super.configure();
        bind(DecompositionStrategy.class).to(StandardDecomposer.class);
        bind(Planner.class).to(JoinPathsPlanner.class);
        bind(JoinOrderPlanner.class).to(ArbitraryJoinOrderPlanner.class);
    }
}
