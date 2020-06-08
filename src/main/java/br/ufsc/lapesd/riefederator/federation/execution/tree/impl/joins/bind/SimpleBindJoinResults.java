package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.CrudeSolutionHashTable;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.QueryExecutionException;
import br.ufsc.lapesd.riefederator.query.modifiers.ValuesModifier;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.FlatMapResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
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

public class SimpleBindJoinResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(SimpleBindJoinResults.class);
    public static final int NOTIFY_DELTA_MS = 5000;
    public static final int DEFAULT_VALUES_ROWS = 40;

    private @Nullable String nodeName = null;
    private final @Nonnull PlanExecutor planExecutor;
    private final @Nonnull Results smaller;
    private final @Nonnull PlanNode rightTree;
    private Results currentResults = null;
    private final @Nonnull Collection<String> joinVars;
    private final @Nonnull Set<String> resultVars;
    private Solution next = null;
    private final int valuesRows = DEFAULT_VALUES_ROWS;
    private final Set<Solution> history = new HashSet<>();
    private int discarded = 0;

    private final @Nonnull Supplier<Results> resultsSupplier;

    private final @Nonnull Stopwatch age = Stopwatch.createUnstarted();
    private final @Nonnull Stopwatch notifyWindow = Stopwatch.createUnstarted();
    private int binds = 0, results = 0;
    private double callBindMs = 0;

    public static class Factory implements BindJoinResultsFactory {
        private final @Nonnull Provider<PlanExecutor> planExecutorProvider;
        private final @Nonnull ResultsExecutor resultsExecutor;

        @Inject
        public Factory(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                       @Nonnull ResultsExecutor resultsExecutor) {
            this.planExecutorProvider = planExecutorProvider;
            this.resultsExecutor = resultsExecutor;
        }

        @Override
        public @Nonnull Results createResults(@Nonnull Results smaller, @Nonnull PlanNode rightTree,
                                              @Nonnull Collection<String> joinVars,
                                              @Nonnull Collection<String> resultVars) {
            PlanExecutor executor = planExecutorProvider.get();
            return new SimpleBindJoinResults(executor, smaller, rightTree, joinVars,
                                             resultVars, resultsExecutor);
        }
    }


    public SimpleBindJoinResults(@Nonnull PlanExecutor planExecutor, @Nonnull Results smaller,
                                 @Nonnull PlanNode rightTree, @Nonnull Collection<String> joinVars,
                                 @Nonnull Collection<String> resultVars,
                                 @Nullable ResultsExecutor resultsExecutor) {
        Preconditions.checkArgument(rightTree.getResultVars().containsAll(joinVars),
                "There are joinVars missing on rightTree");
        this.planExecutor = planExecutor;
        this.rightTree = rightTree;
        this.joinVars = joinVars;
        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : new HashSet<>(resultVars);
        if (canValuesBind(rightTree)) {
            if (!smaller.isAsync() && resultsExecutor != null)
                smaller = resultsExecutor.async(singleton(smaller), valuesRows);
            resultsSupplier = new ValuesBind();
        } else {
            resultsSupplier = new NaiveBind();
        }
        this.smaller = smaller;
    }

    @Override
    public @Nullable String getNodeName() {
        return nodeName;
    }

    @Override
    public void setNodeName(@Nonnull String name) {
        nodeName = name;
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
                     "Right-side rewrite avg: {}ms. Age: {}s. {} duplicates discarded",
                     getNodeName(), exhausted ? "exhausted" : "", results, binds, resultsPerBind,
                     rewriteAvg, age.elapsed(MILLISECONDS)/1000.0, discarded);
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
            return new TransformedResults(rightResults, resultVars, this::reassemble);
        }

        private @Nonnull PlanNode bind(@Nonnull PlanNode node, @Nonnull Solution values) {
            MapSolution.Builder builder = MapSolution.builder();
            for (String name : joinVars) {
                Term term = values.get(name);
                if (term == null)
                    logger.warn("Left Solution is missing join variable {}", name);
                else
                    builder.put(name, term);
            }
            return node.createBound(builder.build());
        }

        private @Nonnull Solution reassemble(@Nonnull Solution right) {
            assert leftSolution != null;
            MapSolution.Builder builder = MapSolution.builder();
            for (String name : resultVars) {
                Term term = leftSolution.get(name, right.get(name));
                if (term != null)
                    builder.put(name, term);
            }
            return builder.build();
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
            return new FlatMapResults(rightResults, resultVars, this::expand);
        }

        private void addLeftSolution(@Nonnull Solution solution) {
            table.add(solution);
            MapSolution.Builder b = MapSolution.builder();
            for (String name : joinVars)
                b.put(name, solution.get(name));
            bindValues.add(b.build());
        }

        private @Nonnull Results expand(@Nonnull Solution right) {
            List<Solution> list = new ArrayList<>(valuesRows*2);
            for (Solution left : table.getAll(right)) {
                MapSolution.Builder builder = MapSolution.builder();
                for (String name : resultVars)
                    builder.put(name, left.get(name, right.get(name)));
                list.add(builder.build());
            }
            return new CollectionResults(list, resultVars);
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
            Solution tmp = currentResults.next();
            if (history.add(MapSolution.builder(tmp).remove("Int").build()))
                next = tmp;
            else
                ++discarded;
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
    public @Nonnull Set<String> getVarNames() {
        return resultVars;
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
