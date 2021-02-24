package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.execution.InjectedExecutor;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.*;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.LazyCartesianOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimplePipeOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimpleQueryOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.DefaultHashJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.DefaultJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.FixedBindJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.FixedHashJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.SequentialResultsExecutor;
import dagger.Module;
import dagger.Provides;
import dagger.Reusable;

import javax.annotation.Nullable;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static java.util.Objects.requireNonNull;

@Module
public abstract class ExecutionModule {
    private static final AtomicInteger threadCounter = new AtomicInteger(0);

    @Provides @Reusable public static PlanExecutor
    planExecutor(@Nullable @Named("override") PlanExecutor override,
                 FreqelConfig config, InjectedExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(PLAN_EXECUTOR, String.class));
        return ModuleHelper.get(PlanExecutor.class, name, def);
    }
    @Provides @Reusable public static QueryOpExecutor
    queryExecutor(@Nullable @Named("override") QueryOpExecutor override,
                  FreqelConfig config, SimpleQueryOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(QUERY_OP_EXECUTOR, String.class));
        return ModuleHelper.get(QueryOpExecutor.class, name, def);

    }
    @Provides @Reusable public static DQueryOpExecutor
    dqueryExecutor(@Nullable @Named("override") DQueryOpExecutor override,
                   FreqelConfig config, SimpleQueryOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(DQUERY_OP_EXECUTOR, String.class));
        return ModuleHelper.get(DQueryOpExecutor.class, name, def);

    }
    @Provides @Reusable public static UnionOpExecutor
    unionExecutor(@Nullable @Named("override") UnionOpExecutor override,
                  FreqelConfig config, SimpleQueryOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(UNION_OP_EXECUTOR, String.class));
        return ModuleHelper.get(UnionOpExecutor.class, name, def);

    }
    @Provides @Reusable public static SPARQLValuesTemplateOpExecutor
    sparqlExecutor(@Nullable @Named("override") SPARQLValuesTemplateOpExecutor override,
                   FreqelConfig config, SimpleQueryOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(SPARQLVALUESTEMPLATE_OP_EXECUTOR, String.class));
        return ModuleHelper.get(SPARQLValuesTemplateOpExecutor.class, name, def);

    }
    @Provides @Reusable public static CartesianOpExecutor
    cartesianExecutor(@Nullable @Named("override") CartesianOpExecutor override,
                      FreqelConfig config, LazyCartesianOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(CARTESIAN_OP_EXECUTOR, String.class));
        return ModuleHelper.get(CartesianOpExecutor.class, name, def);

    }
    @Provides @Reusable public static EmptyOpExecutor
    emptyExecutor(@Nullable @Named("override") EmptyOpExecutor override,
                  FreqelConfig config) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(EMPTY_OP_EXECUTOR, String.class));
        return ModuleHelper.get(EmptyOpExecutor.class, name);

    }
    @Provides @Reusable public static PipeOpExecutor
    pipeExecutor(@Nullable @Named("override") PipeOpExecutor override,
                 FreqelConfig config, SimplePipeOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(PIPE_OP_EXECUTOR, String.class));
        return ModuleHelper.get(PipeOpExecutor.class, name, def);
    }

    @Provides @Singleton public static ResultsExecutor
    resultsExecutor(@Named("override") @Nullable ResultsExecutor override, FreqelConfig config) {
        if (override != null)
            return override;
        double factor = config.get(RESULTS_EXECUTOR_CONCURRENCY_FACTOR, Double.class);
        int cores = Runtime.getRuntime().availableProcessors();
        int max = factor == -1 ? Integer.MAX_VALUE : (int)Math.ceil(cores * factor);
        if (max > 1) {
            assert max > 100 : "max threads too low, may cause starvation";
            BlockingQueue<Runnable> queue = factor == -1 ? new SynchronousQueue<>()
                    : new LinkedBlockingQueue<>();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    0, max, 60, TimeUnit.SECONDS, queue, r -> {
                String name = "ResultsExecutor-" + threadCounter.incrementAndGet();
                Thread t = new Thread(r, name);
                t.setDaemon(true);
                return t;
            });
            int bufferSize = config.get(RESULTS_EXECUTOR_BUFFER_SIZE, Integer.class);
            return new BufferedResultsExecutor(executor, bufferSize);
        } else {
            return new SequentialResultsExecutor();
        }
    }

    @Provides @Reusable public static HashJoinResultsFactory
    hashJoinResultsFactory(@Nullable @Named("override") HashJoinResultsFactory override,
                           FreqelConfig config) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(HASH_JOIN_RESULTS_FACTORY, String.class));
        return ModuleHelper.get(HashJoinResultsFactory.class, name);
    }

    @Provides @Reusable public static BindJoinResultsFactory
    bindJoinResultsFactory(@Nullable @Named("override") BindJoinResultsFactory override,
                           FreqelConfig config,
                           SimpleBindJoinResults.Factory simpleFac) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(BIND_JOIN_RESULTS_FACTORY, String.class));
        return ModuleHelper.get(BindJoinResultsFactory.class, name, simpleFac);
    }

    @Provides @Reusable public static JoinOpExecutor
    joinExecutor(@Nullable @Named("override") JoinOpExecutor override,
                 FreqelConfig config,
                 FixedHashJoinOpExecutor fixedHash, FixedBindJoinOpExecutor fixedBind,
                 DefaultHashJoinOpExecutor defHash, DefaultJoinOpExecutor def) {
        if (override != null)
            return override;
        String name = requireNonNull(config.get(JOIN_OP_EXECUTOR, String.class));
        return ModuleHelper.get(JoinOpExecutor.class, name, fixedHash, defHash, fixedBind, def);
    }
}
