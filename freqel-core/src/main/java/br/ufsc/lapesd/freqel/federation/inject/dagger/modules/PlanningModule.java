package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.concurrent.PoolPlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.EvenAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.ParallelStandardAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.StandardAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.decomp.match.ParallelSourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.federation.decomp.match.SourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.federation.planner.*;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.JoinPathsConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerDispatcher;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetNoInputsConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.freqel.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.freqel.federation.planner.post.PhasedPostPlanner;
import br.ufsc.lapesd.freqel.federation.planner.post.steps.*;
import br.ufsc.lapesd.freqel.federation.planner.pre.PhasedPrePlanner;
import br.ufsc.lapesd.freqel.federation.planner.pre.steps.*;
import br.ufsc.lapesd.freqel.federation.planner.utils.DefaultFilterJoinPlanner;
import br.ufsc.lapesd.freqel.federation.planner.utils.FilterJoinPlanner;
import dagger.Module;
import dagger.Provides;
import dagger.Reusable;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static java.util.Objects.requireNonNull;

@Module
public abstract class PlanningModule {
    @Provides @Singleton public static PerformanceListener
    performanceListener(@Named("override") @Nullable PerformanceListener override,
                        FreqelConfig config) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(PERFORMANCE_LISTENER, String.class));
        return ModuleHelper.get(PerformanceListener.class, name);
    }

    @Provides @Singleton public static MatchingStrategy
    matching(@Named("override") @Nullable MatchingStrategy override,
             FreqelConfig config,
             SourcesListMatchingStrategy sourcesList,
             ParallelSourcesListMatchingStrategy parallel) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(MATCHING, String.class));
        return ModuleHelper.get(MatchingStrategy.class, name, sourcesList, parallel);
    }

    @Provides @Singleton public static Agglutinator
    agglutinator(@Named("override") @Nullable Agglutinator override, FreqelConfig config,
                 StandardAgglutinator std, ParallelStandardAgglutinator parStd,
                 EvenAgglutinator even) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(AGGLUTINATOR, String.class));
        return ModuleHelper.get(Agglutinator.class, name, std, parStd, even);
    }

    @Provides @Reusable public static ConjunctivePlanner
    conjunctivePlanner(@Named("override") @Nullable ConjunctivePlanner override,
                       FreqelConfig config, BitsetConjunctivePlannerDispatcher bsDispatcher,
                       BitsetConjunctivePlanner bs, BitsetNoInputsConjunctivePlanner bsNI,
                       JoinPathsConjunctivePlanner jp) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(CONJUNCTIVE_PLANNER, String.class));
        return ModuleHelper.get(ConjunctivePlanner.class, name, bsDispatcher, bs, bsNI, jp);
    }

    @Provides @Reusable public static EquivCleaner
    equivCleaner(@Named("override") @Nullable EquivCleaner override, FreqelConfig config) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(EQUIV_CLEANER, String.class));
        return ModuleHelper.get(EquivCleaner.class, name);
    }

    @Provides @Reusable public static JoinOrderPlanner
    joinOrderPlanner(@Named("override") @Nullable JoinOrderPlanner override, FreqelConfig config,
                     GreedyJoinOrderPlanner greedy, ArbitraryJoinOrderPlanner arbitrary) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(JOIN_ORDER_PLANNER, String.class));
        return ModuleHelper.get(JoinOrderPlanner.class, name, greedy, arbitrary);
    }

    @Provides @Reusable public static @Named("planningCoreThreads") int
    planningCoreThreads(@Named("planningCoreThreadsOverride") @Nullable Integer override,
                        FreqelConfig config) {
        return override != null ? override : config.get(PLANNING_CORE_THREADS, Integer.class);
    }

    @Provides @Reusable public static @Named("planningMaxThreads") int
    planningMaxThreads(@Named("planningMaxThreadsOverride") @Nullable Integer override,
                        FreqelConfig config) {
        return override != null ? override : config.get(PLANNING_MAX_THREADS, Integer.class);
    }

    @Provides @Singleton public static PlanningExecutorService
    planningExecutorService(@Named("override") @Nullable PlanningExecutorService override,
                            FreqelConfig config,
                            Provider<PoolPlanningExecutorService> poolProvider) {
        if (override != null)
            return override;
        String poolName = PoolPlanningExecutorService.class.getName();
        String name = requireNonNull(config.get(PLANNING_EXECUTOR, String.class));
        if (name.equals(poolName) || poolName.endsWith(name))
            return poolProvider.get();
        return ModuleHelper.get(PlanningExecutorService.class, name);
    }

    @Provides @Reusable public static PrePlanner
    prePlanner(@Named("override") @Nullable PrePlanner override, FreqelConfig config,
               PerformanceListener performanceListener) {
        if (override != null)
            return override;
        List<PlannerShallowStep> shallowSteps = new ArrayList<>();
        if (config.get(PREPLANNER_FLATTEN, Boolean.class))
            shallowSteps.add(new FlattenStep());
        if (config.get(PREPLANNER_CARTESIAN_INTRODUCTION, Boolean.class))
            shallowSteps.add(new CartesianIntroductionStep());
        if (config.get(PREPLANNER_UNION_DISTRIBUTION, Boolean.class))
            shallowSteps.add(new UnionDistributionStep());
        if (config.get(PREPLANNER_CARTESIAN_DISTRIBUTION, Boolean.class))
            shallowSteps.add(new CartesianDistributionStep());

        List<PlannerStep> deepSteps = new ArrayList<>();
        if (config.get(PREPLANNER_PUSH_FILTERS, Boolean.class))
            deepSteps.add(new PushFiltersStep());

        return new PhasedPrePlanner(performanceListener)
                .addShallowPhase(shallowSteps)
                .addDeepPhase(deepSteps);
    }

    @Provides @Reusable public static FilterJoinPlanner
    filterJoinPlanner(@Named("override") @Nullable FilterJoinPlanner override,
                      FreqelConfig config, DefaultFilterJoinPlanner def) {
        if (override != null)
            return override;
        String fallback = DefaultFilterJoinPlanner.class.getName();
        String name = requireNonNull(config.get(FILTER_JOIN_PLANNER, String.class));
        if (name.equals(fallback) || fallback.endsWith(name))
            return def;
        return ModuleHelper.get(FilterJoinPlanner.class, name, def);
    }

    @Provides @Reusable public static PostPlanner
    postPlanner(@Named("override") @Nullable PostPlanner override, FreqelConfig config,
                PerformanceListener performanceListener,
                Provider<ConjunctionReplaceStep> conjunctionReplaceStep,
                Provider<FilterToBindJoinStep> filterToBindJoinStep) {
        if (override != null)
            return override;
        List<PlannerStep> deepSteps = new ArrayList<>();
        if (config.get(POSTPLANNER_CONJUNCTION_REPLACE, Boolean.class))
            deepSteps.add(conjunctionReplaceStep.get());
        if (config.get(POSTPLANNER_FILTER2BIND, Boolean.class))
            deepSteps.add(filterToBindJoinStep.get());
        if (config.get(POSTPLANNER_PUSH_DISTINCT, Boolean.class))
            deepSteps.add(new PushDistinctStep());
        if (config.get(POSTPLANNER_PUSH_LIMIT, Boolean.class))
            deepSteps.add(new PushLimitStep());
        if (config.get(POSTPLANNER_PIPE_CLEANER, Boolean.class))
            deepSteps.add(new PipeCleanerStep());
        if (config.get(POSTPLANNER_PUSH_DISJUNCTIVE, Boolean.class))
            deepSteps.add(new PushDisjunctiveStep());
        return new PhasedPostPlanner(performanceListener).addDeepPhase(deepSteps);
    }
}
