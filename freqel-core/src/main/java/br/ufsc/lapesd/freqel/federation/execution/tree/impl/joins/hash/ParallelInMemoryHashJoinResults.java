package br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class ParallelInMemoryHashJoinResults extends AbstractResults implements Results {
    private static final @Nonnull Logger logger =
            LoggerFactory.getLogger(ParallelInMemoryHashJoinResults.class);

    private final @Nonnull Side[] sides;
    private boolean stop = false;
    private final @Nonnull ExecutorService executorService;
    private final @Nonnull BlockingQueue<Solution> queue = new ArrayBlockingQueue<>(1024);
    private final @Nonnull ArraySolution.ValueFactory solFac;

    private class Side {
        private Future<?> task = null;
        private final @Nonnull CrudeSolutionHashTable table;
        private final @Nonnull Results results;
        private boolean complete = false;
        private final boolean optional;
        private final int idx;

        public Side(@Nonnull Collection<String> joinVars, @Nonnull Results results, int idx) {
            this.results = results;
            this.idx = idx;
            this.table = new CrudeSolutionHashTable(joinVars,  512);
            this.table.recordFetches();
            this.optional = results.isOptional();
        }

        public void start() {
            this.task = executorService.submit(this::fetchTask);
        }

        protected void fetchTask() {
            int otherIdx = (idx + 1) % 2;
            try {
                Side otherSide = sides[otherIdx];
                while (!stop && results.hasNext()) {
                    Solution next = results.next();
                    synchronized (ParallelInMemoryHashJoinResults.this) {
                        CrudeSolutionHashTable.AddedHandle handle = null;
                        if (!otherSide.complete || otherSide.optional)
                            handle = table.add(next);
                        Collection<Solution> otherSolutions = otherSide.table.getAll(next);
                        for (Solution otherSolution : otherSolutions)
                            queue.add(solFac.fromSolutions(next, otherSolution));
                        if (!otherSolutions.isEmpty()) {
                            ParallelInMemoryHashJoinResults.this.notify();
                            if (handle != null)
                                handle.markFetched();
                        }
                    }
                }
                if (!stop) {
                    synchronized (ParallelInMemoryHashJoinResults.this) {
                        complete = true;
                        if (otherSide.complete) { //add optional solutions
                            if (otherSide.optional)
                                table.forEachNotFetched(s -> queue.add(solFac.fromSolution(s)));
                            if (optional) {
                                otherSide.table.forEachNotFetched(
                                        s -> queue.add(solFac.fromSolution(s)));
                            }
                            table.clear();
                            otherSide.table.clear();
                        }
                        // clear tables early if not OPTIONAL
                        if (!optional)
                            otherSide.table.clear();
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
        super(resultVars);
        Set<String> allVars = Stream.concat(left.getVarNames().stream(),
                                            right.getVarNames().stream()).collect(toSet());
        Preconditions.checkArgument(allVars.containsAll(joinVars));
        solFac = ArraySolution.forVars(resultVars);

        executorService = new ThreadPoolExecutor(0, 2,
                0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(8));
        sides = new Side[] {new Side(joinVars, left, 0), new Side(joinVars, right, 1)};
        sides[0].start();
        sides[1].start();
    }

    @Override
    public boolean isAsync() {
        return true;
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

