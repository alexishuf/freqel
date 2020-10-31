package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.riefederator.federation.cardinality.JoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.BindJoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.LimitCardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.QuickSelectivityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.riefederator.federation.concurrent.PoolPlanningExecutorService;
import br.ufsc.lapesd.riefederator.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.riefederator.federation.decomp.agglutinator.StandardAgglutinator;
import br.ufsc.lapesd.riefederator.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.match.SourcesListMatchingStrategy;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.PostPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.PrePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerDispatcher;
import br.ufsc.lapesd.riefederator.federation.planner.post.PhasedPostPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.post.steps.*;
import br.ufsc.lapesd.riefederator.federation.planner.pre.PhasedPrePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.pre.steps.*;
import br.ufsc.lapesd.riefederator.federation.planner.utils.FilterJoinPlanner;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Collections;

import static java.util.Arrays.asList;

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
        bind(MatchingStrategy.class).to(SourcesListMatchingStrategy.class);
        bind(Agglutinator.class).to(StandardAgglutinator.class);
        bind(ConjunctivePlanner.class).to(BitsetConjunctivePlannerDispatcher.class);
        bind(JoinOrderPlanner.class).to(GreedyJoinOrderPlanner.class);
        bind(PlanningExecutorService.class).toInstance(new PoolPlanningExecutorService());
        configureCardinalityEstimation();
    }

    public static void configureCardinalityEstimation(@Nonnull Binder binder, int estimatePolicy) {
        binder.bind(JoinCardinalityEstimator.class).to(BindJoinCardinalityEstimator.class);
        binder.bind(CardinalityEnsemble.class).to(WorstCaseCardinalityEnsemble.class);

        Multibinder<CardinalityHeuristic> mBinder
                = Multibinder.newSetBinder(binder, CardinalityHeuristic.class);
        mBinder.addBinding().toInstance(new QuickSelectivityHeuristic());
        if (estimatePolicy != 0) {
            mBinder.addBinding()
                    .toInstance(new LimitCardinalityHeuristic(EstimatePolicy.local(100)));
        }
    }

    private void configureCardinalityEstimation() {
        configureCardinalityEstimation(binder(), estimatePolicy);
    }

    public static class DefaultPrePlannerProvider implements Provider<PrePlanner> {
        private @Nonnull PerformanceListener performanceListener;

        @Inject
        public DefaultPrePlannerProvider(@Nonnull PerformanceListener performanceListener) {
            this.performanceListener = performanceListener;
        }

        @Override
        public @Nonnull PrePlanner get() {
            return new PhasedPrePlanner(performanceListener)
                    .addShallowPhase(asList(new FlattenStep(),
                                            new CartesianIntroductionStep(),
                                            new UnionDistributionStep(),
                                            new CartesianDistributionStep()))
                    .addDeepPhase(Collections.singletonList(new PushFiltersStep()));
        }
    }

    public static class DefaultPostPlannerProvider implements Provider<PostPlanner> {
        private @Nonnull final PerformanceListener performanceListener;
        private @Nonnull final JoinOrderPlanner joinOrderPlanner;
        private @Nonnull final FilterJoinPlanner filterJoinPlanner;

        @Inject
        public DefaultPostPlannerProvider(@Nonnull PerformanceListener performanceListener,
                                          @Nonnull JoinOrderPlanner joinOrderPlanner,
                                          @Nonnull FilterJoinPlanner filterJoinPlanner) {
            this.performanceListener = performanceListener;
            this.joinOrderPlanner = joinOrderPlanner;
            this.filterJoinPlanner = filterJoinPlanner;
        }

        @Override
        public @Nonnull PostPlanner get() {
            return new PhasedPostPlanner(performanceListener)
                    .addDeepPhase(asList(
                            // this first step can be run on shallow mode, but would be the
                            // only step. Thus, it is simpler to run in the deep phase
                            new ConjunctionReplaceStep(joinOrderPlanner),
                            new FilterToBindJoinStep(filterJoinPlanner),
                            new PushDistinctStep(),
                            new PushLimitStep(),
                            new PipeCleanerStep(),
                            new EndpointPushStep()
                    ));

        }
    }

    protected void configurePrePlanner() {
        bind(PrePlanner.class).toProvider(DefaultPrePlannerProvider.class);
    }

    protected void configurePostPlanner() {
        bind(PostPlanner.class).toProvider(DefaultPostPlannerProvider.class);
    }
}
