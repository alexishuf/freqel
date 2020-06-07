package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class ParallelInMemoryHashJoinResults implements Results {
    private static final @Nonnull Logger logger =
            LoggerFactory.getLogger(ParallelInMemoryHashJoinResults.class);

    private @Nullable String nodeName;
    private final  @Nonnull Set<String> resultVars;
    private final @Nonnull Side[] sides;
    private boolean stop = false;
    private final @Nonnull ExecutorService executorService;
    private final @Nonnull BlockingQueue<Solution> queue = new ArrayBlockingQueue<>(1024);

    private class Side {
        private Future<?> task = null;
        private final @Nonnull CrudeSolutionHashTable table;
        private final @Nonnull Results results;
        private boolean complete = false;
        private final int idx;

        public Side(@Nonnull Collection<String> joinVars, @Nonnull Results results, int idx) {
            this.results = results;
            this.idx = idx;
            table = new CrudeSolutionHashTable(joinVars,  512);
        }

        public void start() {
            this.task = executorService.submit(this::fetchTask);
        }

        protected void fetchTask() {
            int otherIdx = (idx + 1) % 2;
            try {
                while (!stop && results.hasNext()) {
                    Solution next = results.next();
                    synchronized (ParallelInMemoryHashJoinResults.this) {
                        if (!sides[otherIdx].complete)
                            table.add(next);
                        for (Solution sol : sides[otherIdx].table.getAll(next)) {
                            MapSolution.Builder builder = MapSolution.builder();
                            for (String name : resultVars)
                                builder.put(name, next.get(name, sol.get(name)));
                            queue.add(builder.build());
                            ParallelInMemoryHashJoinResults.this.notify();
                        }
                    }
                }
                if (!stop) {
                    synchronized (ParallelInMemoryHashJoinResults.this) {
                        complete = true;
                        sides[otherIdx].table.clear();
                        ParallelInMemoryHashJoinResults.this.notifyAll();
                    }
                }
            } catch (Exception e) {
                logger.error("fetchTask {} failed with exception.", idx, e);
            }
        }

        public void close(boolean keepInterrupt) throws ResultsCloseException {
            try {
                if (task != null)
                    task.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Interrupted before fetchTask {} could finish", idx);
                if (keepInterrupt)
                    Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                logger.error("fetchTask {} threw. Will proceed with close()", idx, e);
            } catch (TimeoutException e) {
                logger.error("fetchTask {} is stuck. Will proceed with close()", idx);
            }
            results.close();
        }
    }

    public static class Factory implements HashJoinResultsFactory {
        @Override
        public @Nonnull Results createResults(@Nonnull Results left, @Nonnull Results right,
                                              @Nonnull Collection<String> joinVars,
                                              @Nonnull Collection<String> resultVars) {
            return new ParallelInMemoryHashJoinResults(left, right, joinVars, resultVars);
        }
    }
    public static final @Nonnull Factory FACTORY = new Factory();

    public ParallelInMemoryHashJoinResults(@Nonnull Results left, @Nonnull Results right,
                                           @Nonnull Collection<String> joinVars,
                                           @Nonnull Collection<String> resultVars) {
        Set<String> allVars = Stream.concat(left.getVarNames().stream(),
                                            right.getVarNames().stream()).collect(toSet());
        Preconditions.checkArgument(allVars.containsAll(joinVars));
        Preconditions.checkArgument(allVars.containsAll(resultVars));

        this.resultVars = resultVars instanceof Set ? (Set<String>)resultVars
                                                    : new HashSet<>(resultVars);
        this.executorService = new ThreadPoolExecutor(0, 2,
                0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(8));
        sides = new Side[] {new Side(joinVars, left, 0), new Side(joinVars, right, 1)};
        sides[0].start();
        sides[1].start();
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
    public int getReadyCount() {
        return queue.size();
    }

    @Override
    public synchronized boolean hasNext() {
        boolean interrupted = false;
        while (queue.isEmpty() && (!sides[0].complete || !sides[1].complete)) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
        return !queue.isEmpty();
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return queue.take();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return resultVars;
    }

    @Override
    public void close() throws ResultsCloseException {
        stop = true;
        executorService.shutdown();
        try {
            sides[0].close(true);
        } finally {
            sides[1].close(true);
        }
    }
}

