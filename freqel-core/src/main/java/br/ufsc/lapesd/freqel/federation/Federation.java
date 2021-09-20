package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.PostPlanner;
import br.ufsc.lapesd.freqel.federation.planner.PrePlanner;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.TemplateExpander;
import br.ufsc.lapesd.freqel.query.annotations.GlobalContextAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics.INIT_SOURCES_MS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * A {@link CQEndpoint} that decomposes queries into the registered {@link TPEndpoint}s.
 */
public class Federation extends AbstractTPEndpoint implements CQEndpoint {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(Federation.class);

    private final @Nonnull FreqelConfig freqelConfig;
    private final @Nonnull MatchingStrategy matchingStrategy;
    private final @Nonnull Agglutinator agglutinator;
    private final @Nonnull PrePlanner prePlanner;
    private final @Nonnull ConjunctivePlanner conjunctivePlanner;
    private final @Nonnull PostPlanner postPlanner;
    private final @Nonnull PlanExecutor executor;
    private final @Nonnull PerformanceListener performance;
    private final @Nonnull InnerCardinalityComputer cardinalityComputer;
    private final @Nonnull ResultsExecutor resultsExecutor;
    private final @Nonnull PlanningExecutorService executorService;
    private final @Nonnull SourceCache sourceCache;
    private final @Nonnull TBox tBox;
    private @Nonnull TemplateExpander templateExpander;

    @Inject
    public Federation(@Nonnull FreqelConfig freqelConfig,
                      @Nonnull PrePlanner prePlanner,
                      @Nonnull ConjunctivePlanner conjunctivePlanner,
                      @Nonnull PostPlanner postPlanner,
                      @Nonnull MatchingStrategy matchingStrategy,
                      @Nonnull Agglutinator agglutinator,
                      @Nonnull PlanExecutor executor,
                      @Nonnull PerformanceListener performance,
                      @Nonnull InnerCardinalityComputer cardinalityComputer,
                      @Nonnull ResultsExecutor resultsExecutor,
                      @Nonnull PlanningExecutorService executorService,
                      @Nonnull SourceCache sourceCache,
                      @Nonnull TBox tBox) {
        this.freqelConfig = freqelConfig;
        this.prePlanner = prePlanner;
        this.conjunctivePlanner = conjunctivePlanner;
        this.postPlanner = postPlanner;
        this.matchingStrategy = matchingStrategy;
        this.agglutinator = agglutinator;
        this.sourceCache = sourceCache;
        this.tBox = tBox;
        agglutinator.setMatchingStrategy(matchingStrategy);
        this.executor = executor;
        this.performance = performance;
        this.cardinalityComputer = cardinalityComputer;
        this.resultsExecutor = resultsExecutor;
        this.templateExpander = new TemplateExpander();
        this.executorService = executorService;
        this.executorService.bind();
    }

    public @Nonnull Federation setTemplateExpander(@Nonnull TemplateExpander templateExpander) {
        this.templateExpander = templateExpander;
        return this;
    }

    public @Nonnull FreqelConfig getFreqelConfig() {
        return freqelConfig;
    }

    public @Nonnull SourceCache getSourceCache() {
        return sourceCache;
    }

    public @Nonnull TBox getTBox() {
        return tBox;
    }

    public @Nonnull ConjunctivePlanner getConjunctivePlanner() {
        return conjunctivePlanner;
    }

    public @Nonnull TemplateExpander getTemplateExpander() {
        return templateExpander;
    }

    public @Nonnull PerformanceListener getPerformanceListener() {
        return performance;
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull Federation addSource(@Nonnull TPEndpoint source) {
        matchingStrategy.addSource(source);
        return this;
    }

    public @Nonnull Collection<TPEndpoint> getSources() {
        return matchingStrategy.getEndpoints();
    }

    /**
     * Starts parallel initialization of all source {@link Description}s and waits for
     * at most timeout units until all initializations are complete.
     *
     * If the timeout expires, false will be returned, but query() can be safely called.
     * All sources that did not yet completed background initialization will simple return
     * empty results from their {@link Description#match(CQuery, MatchReasoning)} calls.
     *
     * @param timeout timeout value
     * @param unit timeout unit
     * @return true iff all sources completed initialization within the timeout
     */
    public boolean initAllSources(int timeout, @Nonnull TimeUnit unit) {
        try (TimeSampler sampler = INIT_SOURCES_MS.createThreadSampler(performance)) {
            int timeoutMs = (int)MILLISECONDS.convert(timeout, unit);
            Stopwatch sw = Stopwatch.createStarted();

            for (TPEndpoint source : getSources())
                source.getDescription().init();

            int allowed = timeoutMs - (int)sw.elapsed(MILLISECONDS);
            int ok = 0, total = getSources().size();
            for (TPEndpoint source : getSources()) {
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
        return query(new QueryOp(query));
    }

    public @Nonnull Results query(@Nonnull Op query) {
        Stopwatch sw = Stopwatch.createStarted();
        Op plan = plan(expandTemplates(query));
        double planMs = sw.elapsed(MICROSECONDS)/1000.0;
        sw.reset().start();
        Results results = execute(plan);
        if (!logger.isDebugEnabled()) {
            logger.info("plan() took {}ms and iterators setup {}ms ",
                        planMs, sw.elapsed(MICROSECONDS)/1000.0);
        }
        return results;
    }

    public @Nonnull Results execute(@Nonnull Op plan) {
        return resultsExecutor.async(executor.executePlan(plan));
    }

    @VisibleForTesting
    @Nonnull Op expandTemplates(@Nonnull Op query) {
        return TreeUtils.replaceNodes(query, null, op -> {
            if (!(op instanceof QueryOp)) return null;
            QueryOp queryOp = (QueryOp) op;
            MutableCQuery old = queryOp.getQuery();
            CQuery expanded = templateExpander.apply(old);
            return expanded == old ? null : queryOp.withQuery(expanded);
        });
    }

    private @Nonnull Op planComponents(@Nonnull Op root, @Nonnull Op query,
                                       @Nonnull InnerCardinalityComputer cardinalityComputer,
                                       @Nonnull GlobalContextAnnotation gCtx) {
        return TreeUtils.replaceNodes(root, cardinalityComputer, op -> {
            if (op.getClass().equals(QueryOp.class)) {
                QueryOp component = (QueryOp) op;
                MutableCQuery cQuery = component.getQuery();
                cQuery.annotate(gCtx);
                Collection<Op> nodes = matchingStrategy.match(cQuery, agglutinator);
                Op componentPlan;
                try (TimeSampler ignored = Metrics.PLAN_MS.createThreadSampler(performance)) {
                    Set<SPARQLFilter> filters = cQuery.getModifiers().filters();
                    FilterAssigner.placeFiltersOnLeaves(nodes, filters);
                    componentPlan = conjunctivePlanner.plan(cQuery, nodes);
                    FilterAssigner.placeInnerBottommost(componentPlan, filters);
                    TreeUtils.copyNonFilter(componentPlan, component.modifiers());
                }
                boolean report = componentPlan instanceof EmptyOp
                        && component.modifiers().optional() == null
                        && !component.getParents().stream().allMatch(UnionOp.class::isInstance);
                if (report) {
                    logger.info("Non-optional query component is unsatisfiable." +
                                "\n  Component:\n    {}\n  Whole query:\n    {}",
                                component.prettyPrint().replace("\n", "\n    "),
                                query.prettyPrint().replace("\n", "\n    "));
                }
                return componentPlan;
            }
            return op;
        });
    }

    public @Nonnull Op plan(@Nonnull CQuery query) {
        return plan(new QueryOp(query));
    }

    public @Nonnull Op plan(@Nonnull Op query) {
        GlobalContextAnnotation gCtx = new GlobalContextAnnotation();
        gCtx.put(GlobalContextAnnotation.USER_QUERY, query);
        try (TimeSampler ignored = Metrics.FULL_PLAN_MS.createThreadSampler(performance)) {
            Stopwatch sw = Stopwatch.createStarted();
            Op root = TreeUtils.deepCopy(query);
            assert root.assertTreeInvariants();
            Op root2 = prePlanner.plan(root);
            if (root2 != root) gCtx.put(GlobalContextAnnotation.USER_QUERY, root = root2);
            assert root.assertTreeInvariants();

            root2 = planComponents(root, query, cardinalityComputer, gCtx);
            if (root2 != root) gCtx.put(GlobalContextAnnotation.USER_QUERY, root = root2);
            assert root.assertTreeInvariants();
            assert keptRootModifiers(root, query);

            root2 = postPlanner.plan(root);
            if (root2 != root) gCtx.put(GlobalContextAnnotation.USER_QUERY, root = root2);
            assert keptRootModifiers(root, query);
            assert root.assertTreeInvariants();

            TreeUtils.nameNodes(root);
            if (logger.isDebugEnabled()) {
                logger.debug("From query to plan in {}ms. Query: \"\"\"{}\"\"\".\nPlan: \n{}",
                        sw.elapsed(MICROSECONDS) / 1000.0, query.prettyPrint(),
                        root.prettyPrint());
            }
            return root;
        }
    }

    private boolean keptRootModifiers(@Nonnull Op plan, @Nonnull Op query) {
        return (query.modifiers().distinct() == null || plan.modifiers().distinct() != null)
                && (query.modifiers().limit() == null || plan.modifiers().limit() != null)
                && (query.modifiers().projection() == null || plan.modifiers().projection() != null)
                && (query.modifiers().ask() == null || plan.modifiers().ask() != null);
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
            case ASK:
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
        executorService.release();
        for (TPEndpoint ep : getSources()) {
            try {
                ep.close(); // to avoid this, use UncloseableTPEndpoint
            } catch (RuntimeException e) {
                logger.error("Source said to close endpoint {} and it failed", ep, e);
            }
        }
        resultsExecutor.close();
        performance.close();
    }
}
