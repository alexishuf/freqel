package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.federation.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.ComponentNode;
import br.ufsc.lapesd.riefederator.federation.tree.EmptyNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.TemplateExpander;
import br.ufsc.lapesd.riefederator.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.HashDistinctResults;
import br.ufsc.lapesd.riefederator.query.results.impl.ProjectingResults;
import br.ufsc.lapesd.riefederator.util.LogUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableCollection;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics.INIT_SOURCES_MS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * A {@link CQEndpoint} that decomposes queries into the registered {@link Source}s.
 */
public class Federation extends AbstractTPEndpoint implements CQEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(Federation.class);

    private final @Nonnull OuterPlanner outerPlanner;
    private final @Nonnull DecompositionStrategy strategy;
    private final @Nonnull PlanExecutor executor;
    private final @Nonnull PerformanceListener performanceListener;
    private final @Nonnull InnerCardinalityComputer cardinalityComputer;
    private final @Nonnull ResultsExecutor resultsExecutor;
    private @Nonnull TemplateExpander templateExpander;

    @Inject
    public Federation(@Nonnull OuterPlanner outerPlanner,
                      @Nonnull DecompositionStrategy strategy,
                      @Nonnull PlanExecutor executor,
                      @Nonnull PerformanceListener performanceListener,
                      @Nonnull InnerCardinalityComputer cardinalityComputer,
                      @Nonnull ResultsExecutor resultsExecutor) {
        this.outerPlanner = outerPlanner;
        this.strategy = strategy;
        this.executor = executor;
        this.performanceListener = performanceListener;
        this.cardinalityComputer = cardinalityComputer;
        this.resultsExecutor = resultsExecutor;
        this.templateExpander = new TemplateExpander();
    }

    public static @Nonnull Federation createDefault() {
        Injector injector = Guice.createInjector(new SimpleFederationModule());
        return injector.getInstance(Federation.class);
    }

    public @Nonnull DecompositionStrategy getDecompositionStrategy() {
        return strategy;
    }

    public @Nonnull Federation setTemplateExpander(@Nonnull TemplateExpander templateExpander) {
        this.templateExpander = templateExpander;
        return this;
    }

    public @Nonnull TemplateExpander getTemplateExpander() {
        return templateExpander;
    }

    public @Nonnull PerformanceListener getPerformanceListener() {
        return performanceListener;
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull Federation addSource(@Nonnull Source source) {
        strategy.addSource(source);
        return this;
    }

    public @Nonnull ImmutableCollection<Source> getSources() {
        return strategy.getSources();
    }

    /**
     * Starts parallel initialization of all source {@link Description}s and waits for
     * at most timeout units until all initializations are complete.
     *
     * If the timeout expires, false will be returned, but query() can be safely called.
     * All sources that did not yet completed background initialization will simple return
     * empty results from their {@link Description#match(CQuery)} calls.
     *
     * @param timeout timeout value
     * @param unit timeout unit
     * @return true iff all sources completed initialization within the timeout
     */
    public boolean initAllSources(int timeout, @Nonnull TimeUnit unit) {
        try (TimeSampler sampler = INIT_SOURCES_MS.createThreadSampler(performanceListener)) {
            int timeoutMs = (int)MILLISECONDS.convert(timeout, unit);
            Stopwatch sw = Stopwatch.createStarted();

            for (Source source : getSources())
                source.getDescription().init();

            int allowed = timeoutMs - (int)sw.elapsed(MILLISECONDS);
            int ok = 0, total = getSources().size();
            for (Source source : getSources()) {
                if (source.getDescription().waitForInit(allowed))
                    ++ok;
                allowed = timeoutMs - (int)sw.elapsed(MILLISECONDS);
            }
            logger.info("Initialized {}/{} source indices in {}ms", ok, total, sampler.getValue());
            return ok == total;
        }
    }

    @Override
    public @Nonnull Results query(@Nonnull CQuery query) {
        Stopwatch sw = Stopwatch.createStarted();
        PlanNode plan = plan(expandTemplates(query));
        double planMs = sw.elapsed(MICROSECONDS)/1000.0;
        sw.reset().start();
        Results results = execute(query, plan);
        if (!logger.isDebugEnabled()) {
            logger.info("plan() took {}ms and iterators setup {}ms ",
                        planMs, sw.elapsed(MICROSECONDS)/1000.0);
        }
        return results;
    }

    @VisibleForTesting
    @Nonnull Results execute(@Nonnull CQuery query, PlanNode plan) {
        Results results = executor.executePlan(plan);

        results = ProjectingResults.applyIf(results, query);
        results = HashDistinctResults.applyIf(results, query);

        return results;
    }

    @VisibleForTesting
    @Nonnull CQuery expandTemplates(@Nonnull CQuery query) {
        ModifierUtils.check(this, query.getModifiers());
        return templateExpander.apply(query);
    }

    public @Nonnull PlanNode plan(@Nonnull CQuery query) {
        Stopwatch sw = Stopwatch.createStarted();
        ModifierUtils.check(this, query.getModifiers());
        PlanNode root = outerPlanner.plan(query);

        Map<PlanNode, PlanNode> map = new HashMap<>();
        TreeUtils.streamPreOrder(root)
                .filter(ComponentNode.class::isInstance).forEach(n -> {
            ComponentNode componentNode = (ComponentNode) n;
            PlanNode componentPlan = strategy.decompose(componentNode.getQuery());
            if (componentPlan instanceof EmptyNode) {
                logger.info("Query is unsatisfiable because one component extracted by the " +
                            "OuterPlanner is unsatisfiable. Query: {}. Component: {}",
                            query, componentNode.getQuery());
            }
            map.put(componentNode, componentPlan);
        });

        if (map.values().stream().anyMatch(EmptyNode.class::isInstance))
            root = new EmptyNode(query); //unsatisfiable
        root = TreeUtils.replaceNodes(root, map, cardinalityComputer);
        TreeUtils.nameNodes(root);

        if (logger.isDebugEnabled()) {
            logger.debug("From query to plan in {}ms. Query: \"\"\"{}\"\"\".\nPlan: \n{}",
                    sw.elapsed(MICROSECONDS)/1000.0, LogUtils.toString(query),
                    root.prettyPrint());
        }
        return root;
    }

    @Override
    public @Nonnull Cardinality estimate(@Nonnull CQuery query, int policy) {
        return Cardinality.UNSUPPORTED;
    }

    @Override
    public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability){
            case DISTINCT:
            case SPARQL_FILTER:
            case PROJECTION:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean hasRemoteCapability(@Nonnull Capability capability) {
        return false;
    }

    @Override
    public void close() {
        resultsExecutor.close();
        performanceListener.close();
    }
}
