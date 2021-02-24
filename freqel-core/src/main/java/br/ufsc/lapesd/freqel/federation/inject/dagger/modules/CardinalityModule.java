package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.algebra.util.CardinalityAdder;
import br.ufsc.lapesd.freqel.algebra.util.RelativeCardinalityAdder;
import br.ufsc.lapesd.freqel.cardinality.*;
import br.ufsc.lapesd.freqel.cardinality.impl.*;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import dagger.Module;
import dagger.Provides;
import dagger.Reusable;
import dagger.multibindings.ElementsIntoSet;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.util.HashSet;
import java.util.Set;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static java.util.Objects.requireNonNull;

@Module
public abstract class CardinalityModule {

    @Provides public static @Reusable @Named("estimatePolicy") Integer
    estimatePolicy(@Named("estimatePolicyOverride") @Nullable Integer override,
                   FreqelConfig config) {
        if (override != null)
            return override;
        int limit = config.get(ESTIMATE_LIMIT, Integer.class);
        int policy = EstimatePolicy.limit(limit);
        if (config.get(ESTIMATE_ASK_LOCAL, Boolean.class))
            policy |= EstimatePolicy.CAN_ASK_LOCAL;
        if (config.get(ESTIMATE_QUERY_LOCAL, Boolean.class))
            policy |= EstimatePolicy.CAN_QUERY_LOCAL;
        if (config.get(ESTIMATE_ASK_REMOTE, Boolean.class))
            policy |= EstimatePolicy.CAN_ASK_REMOTE;
        if (config.get(ESTIMATE_QUERY_REMOTE, Boolean.class))
            policy |= EstimatePolicy.CAN_QUERY_REMOTE;
        return policy;
    }

    @Provides @Reusable @ElementsIntoSet public static Set<CardinalityHeuristic>
    cardinalityHeuristics(@Named("override") @Nullable Set<CardinalityHeuristic> override,
                          FreqelConfig config, LimitCardinalityHeuristic limit) {
        @SuppressWarnings("unchecked")
        Set<String> names = (Set<String>) config.get(CARDINALITY_HEURISTICS, Set.class);
        assert names != null;
        assert !names.isEmpty();
        Set<CardinalityHeuristic> set = new HashSet<>();
        for (String name : names)
            set.add(ModuleHelper.get(CardinalityHeuristic.class, name, limit));
        return set;
    }

    @Provides @Reusable public static @Named("fast") CardinalityHeuristic
    fastCardinalityHeuristic(@Named("fastOverride") @Nullable CardinalityHeuristic override,
                             FreqelConfig config, LimitCardinalityHeuristic limit) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(FAST_CARDINALITY_HEURISTIC, String.class));
        return ModuleHelper.get(CardinalityHeuristic.class, name, limit);
    }

    @Provides @Reusable public static @Named("relCardAdder.neMin") int
    relCardAdderNEMin(@Named("relCardAdder.neMinOverride") @Nullable Integer override,
                      FreqelConfig config) {
        return override != null ? override
                                : config.get(REL_CARDINALITY_ADDER_NONEMPTY_MIN, Integer.class);
    }

    @Provides @Reusable public static @Named("relCardAdder.neProportion") double
    relCardAdderNEProportion(@Named("relCardAdder.neProportionOverride") @Nullable Double override,
                             FreqelConfig cfg) {
        return override != null ? override
                                : cfg.get(REL_CARDINALITY_ADDER_NONEMPTY_PROPORTION, Double.class);
    }

    @Provides @Reusable public static @Named("relCardAdder.unsProportion") double
    relCardAdderUnsProportion(@Named("relCardAdder.unsProportionOverride") @Nullable Double override,
                              FreqelConfig cfg) {
        return override != null
                ? override : cfg.get(REL_CARDINALITY_ADDER_UNSUPPORTED_PROPORTION, Double.class);
    }

    @Provides @Reusable public static CardinalityAdder
    cardinalityAdder(@Named("override") @Nullable CardinalityAdder override,
                     FreqelConfig config, RelativeCardinalityAdder relative) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(CARDINALITY_ADDER, String.class));
        return ModuleHelper.get(CardinalityAdder.class, name, relative);
    }

    @Provides @Reusable public static InnerCardinalityComputer
    innerCardinalityComputer(@Named("override") @Nullable InnerCardinalityComputer override,
                             FreqelConfig config,
                             DefaultInnerCardinalityComputer def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(INNER_CARDINALITY_COMPUTER, String.class));
        return ModuleHelper.get(InnerCardinalityComputer.class, name, def);
    }

    @Provides @Reusable public static JoinCardinalityEstimator
    joinCardinalityEstimator(@Named("override") @Nullable JoinCardinalityEstimator override,
                             FreqelConfig config, BindJoinCardinalityEstimator bind,
                             AverageJoinCardinalityEstimator avg) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(JOIN_CARDINALITY_ESTIMATOR, String.class));
        return ModuleHelper.get(JoinCardinalityEstimator.class, name, bind, avg);
    }

    @Provides @Reusable public static @Named("largeCardinalityThreshold") Integer
    largeCardinalityThreshold(FreqelConfig config) {
        return config.get(LARGE_CARDINALITY_THRESHOLD, Integer.class);
    }

    @Provides @Reusable public static @Named("hugeCardinalityThreshold") Integer
    hugeCardinalityThreshold(FreqelConfig config) {
        return config.get(HUGE_CARDINALITY_THRESHOLD, Integer.class);
    }

    @Provides @Reusable public static CardinalityComparator
    cardinalityComparator(@Named("override") @Nullable CardinalityComparator override,
                          FreqelConfig config, ThresholdCardinalityComparator def) {
        String name = requireNonNull(config.get(CARDINALITY_COMPARATOR, String.class));
        return ModuleHelper.get(CardinalityComparator.class, name, def);
    }

    @Provides @Reusable public static CardinalityEnsemble
    cardinalityEnsemble(@Named("override") @Nullable CardinalityEnsemble override,
                        FreqelConfig config, FixedCardinalityEnsemble fixed,
                        WorstCaseCardinalityEnsemble worst) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(CARDINALITY_ENSEMBLE, String.class));
        return ModuleHelper.get(CardinalityEnsemble.class, name, fixed, worst);
    }

}
