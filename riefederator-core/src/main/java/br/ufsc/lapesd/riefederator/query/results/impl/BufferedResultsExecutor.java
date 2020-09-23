package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.*;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toSet;

public class BufferedResultsExecutor implements ResultsExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BufferedResultsExecutor.class);
    private static final int DEFAULT_PER_INPUT_BUFFER = 10;

    private final @Nonnull ExecutorService executorService;
    private final int perInputBufferSize;

    @VisibleForTesting
    public BufferedResultsExecutor(@Nonnull ExecutorService executorService,
                                   int perInputBufferSize) {
        this.executorService = executorService;
        this.perInputBufferSize = perInputBufferSize;
    }

    public BufferedResultsExecutor(int perInputBufferSize) {
        this(Executors.newCachedThreadPool(), perInputBufferSize);
    }

    public BufferedResultsExecutor() {
        this(DEFAULT_PER_INPUT_BUFFER);
    }

    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> coll,
                                  @Nullable Collection<String> namesHint) {
        return async(coll, namesHint, perInputBufferSize);
    }

    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> coll,
                                  @Nullable Collection<String> namesHint, int buffer) {
        Collection<String> names = namesHint != null ? namesHint
                        : coll.stream().flatMap(r -> r.getVarNames().stream()).collect(toSet());
        if (closed) {
            logger.error("Calling async() after close()! Will return empty results");
            return CollectionResults.empty(names);
        }
        if (coll.isEmpty())
            return CollectionResults.empty(names);

        List<FeedTask> list = new ArrayList<>(coll.size());
        BlockingQueue<FeedTask.Message> queue = new LinkedBlockingQueue<>();
        boolean distinct = false;
        int idx = 0;
        for (Results results : coll) {
            distinct = results.isDistinct();
            FeedTask task = new FeedTask(idx++, results, queue, buffer);
            list.add(task);
            task.schedule();
        }
        if (idx > 1)
            distinct = false;
        if (closed)
            logger.error("Race: close() called during async()! Will discard solutions");
        boolean projecting = false;
        if (namesHint != null) {
            Set<String> set = names instanceof Set ? (Set<String>)names : FullIndexSet.from(names);
            projecting = coll.stream().anyMatch(r -> !r.getVarNames().equals(set));
            names = set;
        }
        return new ConsumingResults(list, queue, names, projecting, distinct);
    }

    private boolean closed;

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        // FeedTasks still executing will be closed when their poll() has a rejected submit()
        for (Runnable runnable : executorService.shutdownNow()) {
            FeedTask task = (FeedTask) runnable;
            try {
                task.close(true);
            } catch (ResultsCloseException e) {
                logger.error("Problem closing {} from its FeedTask at close(). Ignoring.", task.in);
            }
        }
        // no need to wait per the interface contract
    }

    @Override
    public boolean awaitTermination(long timeout,
                                    @Nonnull TimeUnit unit) throws InterruptedException {
        return executorService.awaitTermination(timeout, unit);
    }

    @Override
    public String toString() {
        return "BufferedAsyncResultsExecutor(" + perInputBufferSize + ")";
    }


    private static void putUninterruptibly(@Nonnull BlockingQueue<FeedTask.Message> queue,
                                           @Nonnull FeedTask.Message value) {
        boolean interrupted = false;
        while (true) {
            try {
                queue.put(value);
                break;
            } catch (InterruptedException e) {
                interrupted = true;
                logger.warn("put({}) was interrupted. This indicates perInputBufferSize " +
                            "is not being honored", value);
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt(); //restore flag
    }

    protected static class ConsumingResults extends AbstractResults {
        private @Nonnull final List<FeedTask> tasks;
        private @Nonnull final BitSet activeTasks;
        private boolean exhausted = false;
        private final boolean distinct;
        private @Nonnull final BlockingQueue<FeedTask.Message> queue;
        private @Nullable ArraySolution.ValueFactory projector;
        private @Nullable Solution next = null;

        public ConsumingResults(@Nonnull List<FeedTask> tasks,
                                @Nonnull BlockingQueue<FeedTask.Message> queue,
                                @Nonnull Collection<String> varNames, boolean projecting,
                                boolean distinct) {
            super(varNames);
            this.tasks = tasks;
            this.activeTasks = new BitSet(tasks.size());
            this.activeTasks.flip(0, tasks.size());
            this.queue = queue;
            this.projector = projecting ? ArraySolution.forVars(varNames) : null;
            this.distinct = distinct;
        }

        @Override
        public boolean isAsync() {
            return true;
        }

        @Override
        public boolean isDistinct() {
            return distinct;
        }

        @Override
        public int getReadyCount() {
            int pendingExhausted = (int)tasks.stream().filter(FeedTask::isExhausted).count()
                                 - (tasks.size() - activeTasks.cardinality());
            int ready = queue.size() - pendingExhausted;
            assert ready >= 0 : "Race between getReadyCount() & hasNext()";
            return ready;
        }

        @Override
        public boolean hasNext() {
            return hasNext(Integer.MAX_VALUE);
        }

        @Override
        public boolean hasNext(int millisecondsTimeout) {
            if (next != null)
                return true;
            boolean interrupted = false;
            while (!exhausted) {
                FeedTask.Message m;
                try {
                    m = queue.poll(millisecondsTimeout, TimeUnit.MILLISECONDS);
//                    m = queue.take();
                } catch (InterruptedException e) {
                    logger.info("Suppressing interrupt of ConsumingResults.hasNext(). Will " +
                                "restore flag upon return");
                    interrupted = true;
                    continue;
                }
                if (m == null)
                    return false; //timed out
                assert m.getTaskId() >= 0 : "Message has negative task id";
                assert m.getTaskId() < tasks.size() : "Message has out of range task id";
                next = m.take();
                if (next == null) {
                    activeTasks.set(m.getTaskId(), false);
                    exhausted = activeTasks.isEmpty();
                } else {
                    break; //has a result
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt(); // restore interrupt flag
            return next != null; //exhausted
        }

        @Override
        public @Nonnull Solution next() {
            if (!hasNext()) throw new NoSuchElementException();
            assert this.next != null;
            Solution next = projector == null ? this.next : projector.fromSolution(this.next);
            this.next = null;
            return next;
        }

        @Override
        public void close() throws ResultsCloseException {
            List<ResultsCloseException> exceptions = new ArrayList<>();
            for (FeedTask task : tasks) {
                try {
                    task.close(false);
                } catch (ResultsCloseException e) { exceptions.add(e); }
            }
            if (exceptions.size() == 1) {
                throw exceptions.get(0);
            } else if (exceptions.size() > 1) {
                ResultsCloseException e;
                e = new ResultsCloseException(this, "Exceptions when closing child Results");
                exceptions.forEach(e::addSuppressed);
                throw e;
            }
        }
    }

    protected class FeedTask implements Runnable {
        private final @Nonnull Results in;
        private final @Nonnull BlockingQueue<Message> queue;
        private final @Nonnull AtomicInteger free;
        private final int id;
        private final int scheduleThreshold;
        private boolean active = false;
        private boolean exhausted = false;

        public class Message {
            private @Nullable final Solution solution;
            private boolean taken = false;

            protected Message(@Nullable Solution solution) {
                this.solution = solution;
            }

            public int getTaskId() {
                return id;
            }

            public @Nullable Solution take() {
                assert !taken : "take() called twice on Message";
                taken = true;
                if (solution != null) // schedule if this is not an exhausted notification
                    scheduleNext();
                assert solution != null || exhausted;
                return solution;
            }

            @Override
            public String toString() {
                return String.format("Message(%d, %s)", id, solution);
            }
        }

        public FeedTask(int id, @Nonnull Results in, @Nonnull BlockingQueue<Message> queue,
                        int bufferSize) {
            this.id = id;
            this.in = in;
            this.queue = queue;
            this.free = new AtomicInteger(bufferSize);
            this.scheduleThreshold = Math.max(1, bufferSize/2);
        }

        public boolean isExhausted() { return exhausted; }

        public void scheduleNext() {
            if (free.incrementAndGet() >= scheduleThreshold)
                schedule();
        }

        public synchronized void schedule() {
            if (!active) { //ensure that this is "enqueued or running" at most once
                active = true;
                try {
                    executorService.execute(this);
                } catch (RejectedExecutionException e) {
                    logger.error("RejectedExecutionException when scheduling execution " +
                                 "over {}. This indicates a race condition with " +
                                 "AsyncResultsExecutor.close() being called concurrently with " +
                                 "async() or with consumption of async() Results.", in);
                    active = false; // no change, no notifyAll()
                    try {
                        close(false);
                    } catch (ResultsCloseException e2) {
                        logger.error("Ignoring exception while handling " +
                                     "RejectedExecutionException", e2);
                    }
                }
            }
        }

        private synchronized boolean acquireFreeSlot() {
            // get a permission (note: take() is not synchronized)
            int e = free.get();
            while (!free.compareAndSet(e, e <= 0 ? e : e-1)) e = free.get();
            if (exhausted || e <= 0) { /* no permission left: become inactive */
                assert e >= 0 : "free had negative value";
                active = false;
                notifyAll(); // close() waits for !active
                return false;
            }
            return true;
        }

        @Override
        public void run() {
            try {
                while (acquireFreeSlot()) {
                    Solution solution = null;
                    try {
                        if (in.hasNext())
                            solution = in.next();
                    } catch (Throwable t) {
                        logger.error("Problem with in.hasNext()/next() for in={}", in, t);
                    }
                    if (solution == null) { //only notify actual state changes
                        exhausted = true;
                        putUninterruptibly(queue, new Message(null));
                    } else {
                        putUninterruptibly(queue, new Message(solution));
                    }
                }
            } catch (Throwable t) {
                logger.error("Unexpected exception", t);
            }
        }

        public void close(boolean forceInactive) throws ResultsCloseException {
            boolean interrupted = false;
            synchronized (this) {
                free.set(0); // disallow new production requests
                if (forceInactive) {
                    active = false;
                    notifyAll(); // wake other threads once we leave the synchronized
                }
                while (active) { //wait until task is not running
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                exhausted = true;
                putUninterruptibly(queue, new Message(null));
            }
            if (interrupted)
                Thread.currentThread().interrupt(); // restore suppressed interrupt
            in.close();
        }
    }
}
