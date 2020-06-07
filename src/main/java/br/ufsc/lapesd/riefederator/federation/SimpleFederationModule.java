package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.LimitCardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.PropertySelectivityCardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.StandardDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinPathsPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.NaiveOuterPlanner;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;

import javax.annotation.Nonnull;

public class SimpleFederationModule extends SimpleExecutionModule {
    private int estimatePolicy = EstimatePolicy.local(100);

    @CanIgnoreReturnValue
    public @Nonnull SimpleFederationModule setLimitEstimatePolicy(int estimatePolicy) {
        this.estimatePolicy = estimatePolicy;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull SimpleFederationModule disableLimitEstimatePolicy() {
        this.estimatePolicy = 0;
        return this;
    }

    @Override @CanIgnoreReturnValue
    public @Nonnull SimpleExecutionModule forceBindJoins() {
        super.forceBindJoins();
        return this;
    }

    @Override
    protected void configure() {
        super.configure();
        bind(OuterPlanner.class).to(NaiveOuterPlanner.class);
        bind(DecompositionStrategy.class).to(StandardDecomposer.class);
        bind(Planner.class).to(JoinPathsPlanner.class);
        bind(JoinOrderPlanner.class).to(GreedyJoinOrderPlanner.class);
        configureCardinalityEstimation();
    }

    public static void configureCardinalityEstimation(@Nonnull Binder binder, int estimatePolicy) {
        binder.bind(CardinalityEnsemble.class).to(WorstCaseCardinalityEnsemble.class);

        Multibinder<CardinalityHeuristic> mBinder
                = Multibinder.newSetBinder(binder, CardinalityHeuristic.class);
        mBinder.addBinding().toInstance(new PropertySelectivityCardinalityHeuristic());
        if (estimatePolicy != 0) {
            mBinder.addBinding()
                    .toInstance(new LimitCardinalityHeuristic(EstimatePolicy.local(100)));
        }
    }

    private void configureCardinalityEstimation() {
        configureCardinalityEstimation(binder(), estimatePolicy);
    }
}
