package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.StandardDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinPathsPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.NaiveOuterPlanner;

public class SimpleFederationModule extends SimpleExecutionModule {
    @Override
    protected void configure() {
        super.configure();
        bind(OuterPlanner.class).to(NaiveOuterPlanner.class);
        bind(DecompositionStrategy.class).to(StandardDecomposer.class);
        bind(Planner.class).to(JoinPathsPlanner.class);
        bind(JoinOrderPlanner.class).to(GreedyJoinOrderPlanner.class);
    }
}
