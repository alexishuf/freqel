package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.CrudeSolutionHashTable;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.QueryExecutionException;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.query.results.*;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.FlatMapResults;
import br.ufsc.lapesd.riefederator.query.results.impl.TransformedResults;
import com.google.common.base.Preconditions;
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

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SimpleBindJoinResults extends AbstractResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(SimpleBindJoinResults.class);
    public static final int NOTIFY_DELTA_MS = 5000;
    public static final int DEFAULT_VALUES_ROWS = 40;

    private final @Nonnull PlanExecutor planExecutor;
    private final @Nonnull Results smaller;
    private final @Nonnull PlanNode rightTree;
    private Results currentResults = null;
    private final @Nonnull Collection<String> joinVars;
    private Solution next = null;
    private final int valuesRows;

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
        private int valuesRows = DEFAULT_VALUES_ROWS;

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
        public @Nonnull Results createResults(@Nonnull Results smaller, @Nonnull PlanNode rightTree,
                                              @Nonnull Collection<String> joinVars,
                                              @Nonnull Collection<String> resultVars) {
            PlanExecutor executor = planExecutorProvider.get();
            return new SimpleBindJoinResults(executor, smaller, rightTree, joinVars,
                                             resultVars, resultsExecutor, valuesRows);
        }
    }


    public SimpleBindJoinResults(@Nonnull PlanExecutor planExecutor, @Nonnull Results smaller,
                                 @Nonnull PlanNode rightTree, @Nonnull Collection<String> joinVars,
                                 @Nonnull Collection<String> resultVars,
                                 @Nullable ResultsExecutor resultsExecutor, int valuesRows) {
        super(resultVars);
        Preconditions.checkArgument(rightTree.getResultVars().containsAll(joinVars),
                "There are joinVars missing on rightTree");
        this.planExecutor = planExecutor;
        this.rightTree = rightTree;
        this.joinVars = joinVars;
        this.valuesRows = valuesRows;
        this.solutionFactory = ArraySolution.forVars(resultVars);
        this.bindSolutionFactory = ArraySolution.forVars(joinVars);
        if (canValuesBind(rightTree)) {
            if (!smaller.isAsync() && resultsExecutor != null)
                smaller = resultsExecutor.async(singleton(smaller), valuesRows*2);
            resultsSupplier = new ValuesBind();
        } else {
            resultsSupplier = new NaiveBind();
        }
        this.smaller = smaller;
    }

    @Override
    public boolean isAsync() {
        return false;
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
            PlanNode bound = bind(rightTree, leftSolution);
            callBindMs += sw.elapsed(MICROSECONDS)/1000.0;
            Results rightResults = planExecutor.executeNode(bound);
            return new TransformedResults(rightResults, varNames, this::reassemble);
        }

        private @Nonnull PlanNode bind(@Nonnull PlanNode node, @Nonnull Solution values) {
            return node.createBound(bindSolutionFactory.fromFunction(values::get));
        }

        private @Nonnull Solution reassemble(@Nonnull Solution right) {
            assert leftSolution != null;
            return solutionFactory.fromSolutions(leftSolution, right);
        }
    }

    private static Stream<QueryNode> streamQNs(PlanNode node) {
        return node instanceof QueryNode ? Stream.of((QueryNode) node)
                : node.getChildren().stream().map(n -> (QueryNode)n);
    }

    private static boolean canValuesBind(PlanNode node) {
        // require a QN or a MQ of QN
        boolean ok = node instanceof QueryNode ||
                ( node instanceof MultiQueryNode
                        && node.getChildren().stream().allMatch(QueryNode.class::isInstance) );
        if (!ok) return false;
        // require that all endpoints support VALUES
        Stream<QueryNode> qns = node instanceof QueryNode ? Stream.of((QueryNode) node)
                : node.getChildren().stream().map(n -> (QueryNode)n);
        return qns.map(QueryNode::getEndpoint).allMatch(e -> e.hasCapability(Capability.VALUES));
    }

    private class ValuesBind implements Supplier<Results> {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(joinVars, valuesRows*10);
        Set<Solution> bindValues = new HashSet<>(valuesRows);

        @Override
        public Results get() {
            table.clear();
            bindValues.clear();
            while (bindValues.size() < valuesRows && smaller.hasNext())
                addLeftSolution(smaller.next());
            ValuesModifier modifier = new ValuesModifier(joinVars, bindValues);
            Stopwatch sw = Stopwatch.createStarted();
            PlanNode rewritten = bind(rightTree, modifier);
            callBindMs += sw.elapsed(MICROSECONDS)/1000.0;
            Results rightResults = planExecutor.executeNode(rewritten);
            return new FlatMapResults(rightResults, varNames, this::expand);
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

        private @Nonnull PlanNode bind(@Nonnull PlanNode node, @Nonnull ValuesModifier modifier) {
            MultiQueryNode.Builder b = MultiQueryNode.builder();
            streamQNs(node).map(qn -> qn.createWithModifier(modifier)).forEach(b::add);
            return b.buildIfMulti();
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
