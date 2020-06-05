package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.HashJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class InMemoryHashJoinResults implements Results {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(HashJoinNodeExecutor.class);

    private @Nonnull Results smaller, larger;
    private @Nonnull Set<String> resultsVars;
    private @Nonnull CrudeSolutionHashTable hashTable;
    private boolean stop = false;
    private @Nonnull ExecutorService executorService;
    private @Nonnull Future<?> fetchTask;
    private @Nonnull ArrayDeque<Solution> queue;
    private boolean atEnd = false;

    public static class Factory implements HashJoinResultsFactory {
        @Override
        public @Nonnull Results createResults(@Nonnull Results left, @Nonnull Results right,
                                              @Nonnull Collection<String> joinVars,
                                              @Nonnull Collection<String> resultVars) {
            return new InMemoryHashJoinResults(left, right, joinVars, resultVars);
        }
    }
    public static final @Nonnull Factory FACTORY = new Factory();

    public InMemoryHashJoinResults(@Nonnull Results smaller, @Nonnull Results larger,
                                   @Nonnull Collection<String> joinVars,
                                   @Nonnull Collection<String> resultVars) {
        Set<String> allVars = Stream.concat(smaller.getVarNames().stream(),
                                            larger.getVarNames().stream()).collect(toSet());
        Preconditions.checkArgument(allVars.containsAll(joinVars));
        Preconditions.checkArgument(allVars.containsAll(resultVars));

        this.hashTable = new CrudeSolutionHashTable(joinVars, 512);
        this.smaller = smaller;
        this.larger = larger;
        this.resultsVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                     : new HashSet<>(resultVars);
        this.executorService = Executors.newSingleThreadExecutor();
        this.fetchTask = executorService.submit(this::fetchAll);
        this.queue = new ArrayDeque<>();
    }

    private void fetchAll() {
        try {
            while (!stop && smaller.hasNext()) {
                hashTable.add(smaller.next());
            }
        } catch (Exception e) {
            logger.error("Fetch Task for {} dying with exception", smaller, e);
        }
    }

    private boolean advance() {
        assert queue.isEmpty();
        /* await uninterruptibly for fetchTask */
        boolean interrupted = false;
        while (!fetchTask.isDone()) {
            try {
                fetchTask.get();
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException ignored) {}
        }
        /* consume larger until we get some matches */
        try {
            while (larger.hasNext()) {
                if (tryJoin(larger.next()))
                    return true;
            }
            atEnd = true;
            return false;
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt(); //restore interrupted flag
        }
    }

    private boolean tryJoin(@Nonnull Solution fromLarger) {
        boolean joined = false;
        for (Solution fromSmaller : hashTable.getAll(fromLarger)) {
            MapSolution.Builder builder = MapSolution.builder();
            for (String name : resultsVars)
                builder.put(name, fromSmaller.get(name, fromLarger.get(name)));
            MapSolution result = builder.build();
            queue.add(result);
            joined = true;
        }
        return joined;
    }

    @Override
    public int getReadyCount() {
        return queue.size();
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty() || advance();
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException("Results exhausted");
        return queue.remove();
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return resultsVars;
    }

    @Override
    public void close() throws ResultsCloseException {
        stop = true;
        try {
            fetchTask.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.error("Fetch task is stuck, giving up on it");
        } catch (InterruptedException e) {
            logger.warn("Interrupted on close() while waiting for fetch task");
        } catch (ExecutionException e) {
            logger.error("Fetch task threw. Will proceed with close()", e);
        }
        executorService.shutdown();
        try {
            larger.close();
        } finally {
            smaller.close();
        }
    }

    @Override
    public @Nonnull String toString() {
        return String.format("InMemoryHashJoinResults@%x", System.identityHashCode(this));
    }
}
