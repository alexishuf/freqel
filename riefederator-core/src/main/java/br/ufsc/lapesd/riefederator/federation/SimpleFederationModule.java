package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.riefederator.federation.cardinality.JoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.BindJoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.GeneralSelectivityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.LimitCardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.StandardDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.PostPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.PrePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.JoinPathsConjunctivePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.post.PhasedPostPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.post.steps.PipeCleanerStep;
import br.ufsc.lapesd.riefederator.federation.planner.pre.PhasedPrePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.pre.steps.*;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

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
        configurePrePlanner();
        configurePostPlanner();
        bind(DecompositionStrategy.class).to(StandardDecomposer.class);
        bind(ConjunctivePlanner.class).to(JoinPathsConjunctivePlanner.class);
        bind(JoinOrderPlanner.class).to(GreedyJoinOrderPlanner.class);
        configureCardinalityEstimation();
    }

    public static void configureCardinalityEstimation(@Nonnull Binder binder, int estimatePolicy) {
        binder.bind(JoinCardinalityEstimator.class).to(BindJoinCardinalityEstimator.class);
        binder.bind(CardinalityEnsemble.class).to(WorstCaseCardinalityEnsemble.class);

        Multibinder<CardinalityHeuristic> mBinder
                = Multibinder.newSetBinder(binder, CardinalityHeuristic.class);
        mBinder.addBinding().toInstance(new GeneralSelectivityHeuristic());
        if (estimatePolicy != 0) {
            mBinder.addBinding()
                    .toInstance(new LimitCardinalityHeuristic(EstimatePolicy.local(100)));
        }
    }

    private void configureCardinalityEstimation() {
        configureCardinalityEstimation(binder(), estimatePolicy);
    }

    public static class DefaultPrePlannerProvider implements Provider<PrePlanner> {
        private @Nonnull JoinOrderPlanner joinOrderPlanner;
        private @Nonnull PerformanceListener performanceListener;

        @Inject
        public DefaultPrePlannerProvider(@Nonnull JoinOrderPlanner joinOrderPlanner,
                                         @Nonnull PerformanceListener performanceListener) {
            this.joinOrderPlanner = joinOrderPlanner;
            this.performanceListener = performanceListener;
        }

        @Override public @Nonnull PrePlanner get() {
            return new PhasedPrePlanner(performanceListener)
                    .appendPhase1(new FlattenStep())
                    .appendPhase1(new CartesianIntroductionStep())
                    .appendPhase1(new FlattenStep())
                    .appendPhase2(new UnionDistributionStep())
                    .appendPhase2(new CartesianDistributionStep())
                    .appendPhase3(new ConjunctionReplaceStep(joinOrderPlanner))
                    .appendPhase3(new FlattenStep())
                    .appendPhase3(new FiltersPushStep());
        }
    }

    public static class DefaultPostPlannerProvider implements Provider<PostPlanner> {
        private @Nonnull final PerformanceListener performanceListener;

        @Inject
        public DefaultPostPlannerProvider(@Nonnull PerformanceListener performanceListener) {
            this.performanceListener = performanceListener;
        }

        @Override public PostPlanner get() {
            return new PhasedPostPlanner(performanceListener).appendPhase1(new PipeCleanerStep());
        }
    }

    protected void configurePrePlanner() {
        bind(PrePlanner.class).toProvider(DefaultPrePlannerProvider.class);
    }

    protected void configurePostPlanner() {
        bind(PostPlanner.class).toProvider(DefaultPostPlannerProvider.class);
    }
}
