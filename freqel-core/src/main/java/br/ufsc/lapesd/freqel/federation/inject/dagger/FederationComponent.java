package br.ufsc.lapesd.freqel.federation.inject.dagger;

import br.ufsc.lapesd.freqel.algebra.util.CardinalityAdder;
import br.ufsc.lapesd.freqel.cardinality.*;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.*;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.freqel.federation.inject.dagger.modules.*;
import br.ufsc.lapesd.freqel.federation.planner.*;
import br.ufsc.lapesd.freqel.federation.planner.utils.FilterJoinPlanner;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxMaterializer;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import dagger.BindsInstance;
import dagger.Component;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
import java.util.Set;

@Component(modules = {
        FreqelConfigModule.class,
        ExecutionModule.class,
        CardinalityModule.class,
        PlanningModule.class,
        SourcesModule.class,
        ReasoningModule.class
})
@Singleton
public interface FederationComponent {
    Federation federation();
    FreqelConfig config();
    SourceLoaderRegistry sourceLoaders();
    
    @Component.Builder
    interface Builder {
        /* --- --- --- Stuff from ExecutionModule --- --- --- */

        @BindsInstance Builder overridePlanExecutor(@Named("override") @Nullable PlanExecutor e);
        @BindsInstance Builder overrideQueryOpExecutor(@Named("override") @Nullable QueryOpExecutor e);
        @BindsInstance Builder overrideDQueryOpExecutor(@Named("override") @Nullable DQueryOpExecutor e);
        @BindsInstance Builder overrideUnionOpExecutor(@Named("override") @Nullable UnionOpExecutor e);
        @BindsInstance Builder overrideSPARQLValuesTemplateOpExecutor(@Named("override") @Nullable SPARQLValuesTemplateOpExecutor e);
        @BindsInstance Builder overrideCartesianOpExecutor(@Named("override") @Nullable CartesianOpExecutor e);
        @BindsInstance Builder overrideEmptyOpExecutor(@Named("override") @Nullable EmptyOpExecutor e);
        @BindsInstance Builder overridePipeOpExecutor(@Named("override") @Nullable PipeOpExecutor e);
        @BindsInstance Builder overrideResultsExecutor(@Named("override") @Nullable ResultsExecutor e);
        @BindsInstance Builder overrideHashJoinResultsFactory(@Named("override") @Nullable HashJoinResultsFactory f);
        @BindsInstance Builder overrideBindJoinResultsFactory(@Named("override") @Nullable BindJoinResultsFactory f);
        @BindsInstance Builder overrideJoinOpExecutor(@Named("override") @Nullable JoinOpExecutor e);

        /* --- --- --- Stuff from FreqelConfigModule --- --- --- */

        @BindsInstance Builder overrideTempDir(@Nullable @Named("tempDirOverride") File dir);
        @BindsInstance Builder overrideFreqelConfig(@Nullable @Named("override") FreqelConfig c);

        /* --- --- --- Stuff from CardinalityModule --- --- --- */

        @BindsInstance Builder overrideEstimatePolicy(@Nullable @Named("estimatePolicyOverride") Integer p);
        @BindsInstance Builder overrideCardinalityAdder(@Nullable @Named("override") CardinalityAdder adder);
        @BindsInstance Builder overrideCardinalityEnsemble(@Nullable @Named("override") CardinalityEnsemble e);
        @BindsInstance Builder overrideEquivCleaner(@Nullable @Named("override") EquivCleaner e);
        @BindsInstance Builder overrideCardinalityHeuristics(@Nullable @Named("override") Set<CardinalityHeuristic> s);
        @BindsInstance Builder overrideFastCardinalityHeuristic(@Nullable @Named("fastOverride") CardinalityHeuristic h);
        @BindsInstance Builder overrideInnerCardinalityComputer(@Nullable @Named("override") InnerCardinalityComputer c);
        @BindsInstance Builder overrideJoinCardinalityEstimator(@Nullable @Named("override") JoinCardinalityEstimator e);
        @BindsInstance Builder overrideCardinalityComparator(@Nullable @Named("override") CardinalityComparator c);
        @BindsInstance Builder overrideRelCardAdderNoneEmptyMin(@Nullable @Named("relCardAdder.neMinOverride") Integer i);
        @BindsInstance Builder overrideRelCardAdderNonEmptyProportion(@Named("relCardAdder.neProportionOverride") @Nullable Double p);
        @BindsInstance Builder overrideRelCardAdderUnsupportedProportion(@Named("relCardAdder.unsProportionOverride") @Nullable Double p);

        /* --- --- --- Stuff from PlanningModule --- --- --- */

        @BindsInstance Builder overrideMatchingStrategy(@Nullable @Named("override") MatchingStrategy m);
        @BindsInstance Builder overridePerformanceListener(@Nullable @Named("override") PerformanceListener l);
        @BindsInstance Builder overrideAgglutinator(@Nullable @Named("override") Agglutinator a);
        @BindsInstance Builder overrideConjunctivePlanner(@Nullable @Named("override") ConjunctivePlanner p);
        @BindsInstance Builder overrideJoinOrderPlanner(@Nullable @Named("override") JoinOrderPlanner p);
        @BindsInstance Builder overrideFilterJoinPlanner(@Nullable @Named("override") FilterJoinPlanner p);
        @BindsInstance Builder overridePrePlanner(@Nullable @Named("override")PrePlanner p);
        @BindsInstance Builder overridePostPlanner(@Nullable @Named("override")PostPlanner p);
        @BindsInstance Builder overridePlanningExecutorService(@Nullable @Named("override") PlanningExecutorService s);
        @BindsInstance Builder overridePlanningCoreThreads(@Nullable @Named("planningCoreThreadsOverride") Integer i);
        @BindsInstance Builder overridePlanningMaxThreads(@Nullable @Named("planningMaxThreadsOverride") Integer i);

        /* --- --- --- Stuff from ReasoningModule --- --- --- */

        @BindsInstance Builder overrideTBox(@Nullable @Named("override") TBox t);
        @BindsInstance Builder overrideTBoxMaterializer(@Nullable @Named("override") TBoxMaterializer m);

        /* --- --- --- Stuff from SourcesModule --- --- --- */

        @BindsInstance Builder overrideSourceCache(@Nullable @Named("override") SourceCache c);
        @BindsInstance Builder overrideSourcesCacheDir(@Nullable @Named("sourceCacheDirOverride") File d);
        @BindsInstance Builder overrideIndexingBackoffStrategy(@Nullable @Named("indexingOverride") BackoffStrategy s);

        FederationComponent build();
    }
}
