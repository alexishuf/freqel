package br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.SPARQLValuesTemplateOp;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.CrudeSolutionHashTable;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.exceptions.QueryExecutionException;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.freqel.query.results.*;
import br.ufsc.lapesd.freqel.query.results.impl.*;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.results.impl.CollectionResults.wrapSameVars;
import static br.ufsc.lapesd.freqel.util.CollectionUtils.union;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SimpleBindJoinResults extends AbstractResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(SimpleBindJoinResults.class);
    public static final int NOTIFY_DELTA_MS = 5000;
    public static final int DEF_VALUES_ROWS = 40;
    public static final int[][] DEF_VALUES_NO_SHORTCUTS = {};
    public static final int[][] DEF_VALUES_SHORTCUTS = {{10, 1}, {2, 4}, {1, 7}};

    private final @Nonnull PlanExecutor planExecutor;
    private final @Nonnull Results smaller;
    private final @Nonnull Op rightTree;
    private Results currentResults = null;
    private final @Nonnull Collection<String> joinVars;
    private Solution next = null;
    private final int valuesRows;
    private final @Nonnull int[][] valuesShortcuts;

    private final @Nonnull ArraySolution.ValueFactory solutionFactory;
    private final @Nonnull ArraySolution.ValueFactory bindSolutionFactory;

    private final @Nonnull Supplier<Results> resultsSupplier;

    private final @Nonnull Stopwatch age = Stopwatch.createUnstarted();
    private final @Nonnull Stopwatch notifyWindow = Stopwatch.createUnstarted();
    private int binds = 0, results = 0;
    private double callBindMs = 0;

    public static class Factory implements BindJoinResultsFactory {
        private final @Nonnull Provider<PlanExecutor> planExecutorProvider;
        private final @Nonnull ResultsExecutor resultsExecutor;
        private int valuesRows = DEF_VALUES_ROWS;

        @Inject
        public Factory(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                       @Nonnull ResultsExecutor resultsExecutor) {
            this.planExecutorProvider = planExecutorProvider;
            this.resultsExecutor = resultsExecutor;
        }

        public void setValuesRows(int valuesRows) {
            this.valuesRows = valuesRows;
        }

        @Override
        public @Nonnull Results createResults(@Nonnull Results smaller, @Nonnull Op rightTree,
                                              @Nonnull Collection<String> joinVars,
                                              @Nonnull Collection<String> resultVars) {
            PlanExecutor executor = planExecutorProvider.get();
            return new SimpleBindJoinResults(executor, smaller, rightTree, joinVars,
                                             resultVars, resultsExecutor, valuesRows);
        }
    }


    public SimpleBindJoinResults(@Nonnull PlanExecutor planExecutor, @Nonnull Results smaller,
                                 @Nonnull Op rightTree, @Nonnull Collection<String> joinVars,
                                 @Nonnull Collection<String> resultVars,
                                 @Nullable ResultsExecutor resultsExecutor, int valuesRows) {
        super(resultVars);
        checkArgument(rightTree.getPublicVars().containsAll(joinVars),
                      "There are joinVars missing on rightTree");
        checkArgument(!smaller.isOptional(), "Cannot provide OPTIONAL semantics for left operand");
        this.planExecutor = planExecutor;
        this.rightTree = rightTree;
        this.joinVars = joinVars;
        this.valuesRows = valuesRows;
        this.solutionFactory = ArraySolution.forVars(resultVars);
        this.bindSolutionFactory = ArraySolution.forVars(joinVars);
        if (canValuesBind(rightTree)) {
            if (!smaller.isAsync() && resultsExecutor != null)
                smaller = resultsExecutor.async(singleton(smaller), smaller.getVarNames(),valuesRows*2);
            valuesShortcuts = smaller.isAsync() ? DEF_VALUES_SHORTCUTS : DEF_VALUES_NO_SHORTCUTS;
            resultsSupplier = new ValuesBind();
        } else {
            resultsSupplier = new NaiveBind();
            valuesShortcuts = DEF_VALUES_NO_SHORTCUTS;
        }
        this.smaller = smaller;
    }

    @Override
    public int getReadyCount() {
        return next != null ? 1 : 0;
    }

    private void logStatus(boolean exhausted) {
        if (!logger.isDebugEnabled()) return;
        if (!exhausted && !notifyWindow.isRunning()) {
            notifyWindow.start();
            return;
        }
        if (!exhausted && notifyWindow.elapsed(MILLISECONDS) < NOTIFY_DELTA_MS)
            return;
        notifyWindow.reset().start();
        double resultsPerBind = binds > 0 ? results / (double)binds : 0;
        double rewriteAvg = binds > 0 ? callBindMs / (double)binds : 0;
        logger.debug("{}: {} {} results from {} binds (avg: {} results/bind). " +
                     "Right-side rewrite avg: {}ms. Age: {}s.",
                     getNodeName(), exhausted ? "exhausted" : "", results, binds, resultsPerBind,
                     rewriteAvg, age.elapsed(MILLISECONDS)/1000.0);
    }

    private class NaiveBind implements Supplier<Results> {
        private @Nullable Solution leftSolution;

        @Override
        public Results get() {
            leftSolution = smaller.next();
            Stopwatch sw = Stopwatch.createStarted();
            Op bound = bind(rightTree, leftSolution);
            callBindMs += sw.elapsed(MICROSECONDS)/1000.0;
            Results rightResults = planExecutor.executeNode(bound);
            TransformedResults results = new TransformedResults(rightResults,
                                                                varNames, this::reassemble);
            if (rightTree.modifiers().optional() != null && !results.hasNext())
                return wrapSameVars(singleton(solutionFactory.fromSolution(leftSolution)));
            return results;
        }

        private @Nonnull Op bind(@Nonnull Op node, @Nonnull Solution values) {
            return node.createBound(bindSolutionFactory.fromFunction(values::get));
        }

        private @Nonnull Solution reassemble(@Nonnull Solution right) {
            assert leftSolution != null;
            return solutionFactory.fromSolutions(leftSolution, right);
        }
    }

    private static Stream<EndpointQueryOp> streamQNs(Op node) {
        return node instanceof EndpointQueryOp ? Stream.of((EndpointQueryOp) node)
                : node.getChildren().stream().map(n -> (EndpointQueryOp)n);
    }

    private static boolean canValuesBind(Op node) {
        // require a QN or a MQ of QN
        boolean ok = node instanceof EndpointQueryOp ||
                ( node instanceof UnionOp
                        && node.getChildren().stream().allMatch(EndpointQueryOp.class::isInstance) );
        if (!ok) return false;
        // require that all endpoints support VALUES
        Stream<EndpointQueryOp> qns = node instanceof EndpointQueryOp ? Stream.of((EndpointQueryOp) node)
                : node.getChildren().stream().map(n -> (EndpointQueryOp)n);
        return qns.map(EndpointQueryOp::getEndpoint).allMatch(e -> e.hasCapability(Capability.VALUES));
    }

    private class ValuesBind implements Supplier<Results> {
        @Nonnull final CrudeSolutionHashTable table;
        Set<Solution> bindValues = new HashSet<>(valuesRows);
        SPARQLValuesTemplateOp template = null;

        public ValuesBind() {
            table = new CrudeSolutionHashTable(joinVars, valuesRows*10);
            if (rightTree.modifiers().optional() != null)
                table.recordFetches();
        }

        @Override
        public Results get() {
            table.clear();
            bindValues.clear();
            initTemplate();
            int shortcut = Integer.MAX_VALUE;
            while (bindValues.size() < valuesRows && smaller.hasNext(shortcut)) {
                addLeftSolution(smaller.next());
                shortcut = getShortcut();
            }
            Stopwatch sw = Stopwatch.createStarted();
            Op rewritten = bind(joinVars, bindValues);
            callBindMs += sw.elapsed(MICROSECONDS)/1000.0;
            Results rightResults = planExecutor.executeNode(rewritten);
            Results results = new FlatMapResults(rightResults, varNames, this::expand);
            return scheduleForOptionalRight(results);
        }

        private Results scheduleForOptionalRight(@Nonnull Results results) {
            if (rightTree.modifiers().optional() == null)
                return results;
            AbstractResults orphans = new AbstractResults(varNames) {
                private ArrayDeque<Solution> queue = null;
                @Override public boolean hasNext() {
                    if (queue == null) {
                        queue = new ArrayDeque<>();
                        table.forEachNotFetched(s -> queue.add(solutionFactory.fromSolution(s)));
                    }
                    return !queue.isEmpty();
                }
                @Override public @Nonnull Solution next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    return queue.remove();
                }
                @Override public void close() { }
            };
            return new SequentialResults(Arrays.asList(results, orphans), varNames);
        }

        private int getShortcut() {
            for (int[] spec : valuesShortcuts) {
                if (bindValues.size() >= spec[0]) {
                    return spec[1];
                }
            }
            return Integer.MAX_VALUE;
        }

        private void addLeftSolution(@Nonnull Solution solution) {
            table.add(solution);
            bindValues.add(bindSolutionFactory.fromFunction(solution::get));
        }

        private @Nonnull Results expand(@Nonnull Solution right) {
            List<Solution> list = new ArrayList<>(valuesRows*2);
            for (Solution left : table.getAll(right))
                list.add(solutionFactory.fromSolutions(left, right));
            return new CollectionResults(list, varNames);
        }

        private void initTemplate() {
            if (template != null) return;

            EndpointQueryOp qn;
            if (rightTree instanceof EndpointQueryOp)
                qn = (EndpointQueryOp) SimpleBindJoinResults.this.rightTree;
            else
                qn = (EndpointQueryOp) rightTree.getChildren().iterator().next();
            TPEndpoint ep = qn.getEndpoint();
            MutableCQuery query = qn.getQuery();
            Set<String> old = query.attr().publicVarNames();
            if (!old.containsAll(joinVars)) {
                query = new MutableCQuery(query);
                query.mutateModifiers().add(Projection.of(union(old, joinVars)));
            }
            template = new SPARQLValuesTemplateOp(ep, query);
            assert template.getResultVars().containsAll(joinVars);
        }

        private @Nonnull Op bind(@Nonnull Collection<String> varNames,
                                 @Nonnull Collection<Solution> assignments) {
            if (rightTree instanceof EndpointQueryOp) {
                CQEndpoint ep = (CQEndpoint) ((EndpointQueryOp) rightTree).getEndpoint();
                if (ep.canQuerySPARQL()) {
                    template.setValues(varNames, assignments);
                    return template;
                } else {
                    ValuesModifier modifier = new ValuesModifier(varNames, assignments);
                    rightTree.modifiers().add(modifier);
                    return rightTree;
                }
            } else {
                UnionOp.Builder b = UnionOp.builder();
                ValuesModifier modifier = null;
                for (Op child : rightTree.getChildren()) {
                    CQEndpoint endpoint = (CQEndpoint) ((EndpointQueryOp) child).getEndpoint();
                    if (endpoint.canQuerySPARQL()) {
                        SPARQLValuesTemplateOp node = template.withEndpoint(endpoint);
                        node.setValues(varNames, assignments);
                        b.add(node);
                    } else {
                        if (modifier == null)
                            modifier = new ValuesModifier(varNames, assignments);
                        child.modifiers().add(modifier);
                        b.add(child);
                    }
                }
                return b.build();
            }
        }

        @Override
        public @Nonnull String toString() {
            int id = System.identityHashCode(this);
            return String.format("ValuesBind@%x[%s]", id, getNodeName());
        }
    }

    private void advance() {
        assert next == null;
        if (!age.isRunning()) age.start();
        while (next == null) {
            while (currentResults == null || !currentResults.hasNext()) {
                if (!smaller.hasNext()) {
                    logStatus(true);
                    return;
                }
                if (currentResults != null) {
                    try {
                        currentResults.close();
                    } catch (ResultsCloseException e) {
                        logger.error("Problem closing rightResults of bound plan tree", e);
                    }
                }
                try {
                    currentResults = resultsSupplier.get();
                } catch (QueryExecutionException e) {
                    logger.error("Failed to execute bind-join query. Will ignore and " +
                            "continue joining", e);
                }
                ++binds;
            }
            next = currentResults.next();
        }
        ++results;
        logStatus(false);
    }

    @Override
    public boolean hasNext() {
        if (next == null)
            advance();
        return next != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        assert next != null;
        Solution solution = next;
        next = null;
        return solution;
    }

    @Override
    public void close() throws ResultsCloseException {
        try {
            smaller.close();
        } finally {
            if (currentResults != null)
                currentResults.close();
        }
    }
}
