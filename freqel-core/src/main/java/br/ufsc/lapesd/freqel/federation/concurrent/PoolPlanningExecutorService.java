package br.ufsc.lapesd.freqel.federation.concurrent;

import com.google.common.util.concurrent.ForwardingExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static java.lang.String.format;
import static java.lang.System.getSecurityManager;
import static java.lang.Thread.currentThread;

public class PoolPlanningExecutorService extends ForwardingExecutorService
        implements PlanningExecutorService {
    private static final @Nonnull Logger logger =
            LoggerFactory.getLogger(PoolPlanningExecutorService.class);
    private static final @Nonnull AtomicInteger nextPoolId = new AtomicInteger(0);

    private ThreadPoolExecutor executor;
    private final int core, max;
    private int references;
    private final @Nonnull ThreadFactory threadFactory = new ThreadFactory() {
        private final @Nonnull AtomicInteger nextThreadId = new AtomicInteger(0);
        private final @Nonnull ThreadGroup group = getSecurityManager() == null
                                                 ? currentThread().getThreadGroup()
                                                 : getSecurityManager().getThreadGroup();
        private final @Nonnull String namePrefix = PoolPlanningExecutorService.class.getSimpleName()
                                                 + "-" + nextPoolId.getAndIncrement();


        @Override public Thread newThread(@Nonnull Runnable r) {
            String name = namePrefix + "-" + nextThreadId.getAndIncrement();
            Thread thread = new Thread(group, r, name, 0);
            if (!thread.isDaemon())
                thread.setDaemon(true);
            if (thread.getPriority() != Thread.NORM_PRIORITY)
                thread.setPriority(Thread.NORM_PRIORITY);
            return thread;
        }
    };

    public PoolPlanningExecutorService() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public PoolPlanningExecutorService(int core) {
        this(core, core + (core/3 + 1));
    }

    @Inject public PoolPlanningExecutorService(@Named("planningCoreThreads") int core,
                                               @Named("planningMaxThreads") int max) {
        this.core = core;
        this.max = max;
        this.references  = 0;
    }

    @Override protected @Nonnull ExecutorService delegate() {
        if (executor == null) throw new IllegalStateException("bind() not called!");
        return executor;
    }

    @Override public synchronized void bind() {
        ++references;
        if (executor == null) {
            assert references > 0;
            assert references == 1;
            executor = new ThreadPoolExecutor(core, max, 30, TimeUnit.SECONDS,
                                              new ArrayBlockingQueue<>(core), threadFactory,
                                              new ThreadPoolExecutor.CallerRunsPolicy());
        }
    }

    @Override public void release() {
        ThreadPoolExecutor old;
        synchronized (this) {
            if (references > 0) {
                --references;
                assert this.executor != null;
            } else {
                assert this.executor == null;
                return;
            }
            if (references > 0)
                return; // do not really shutdown
            old = this.executor;
            this.executor = null;
        }
        old.shutdown();
        try {
            if (!old.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("{}.release() timed ou on executor.awaitTermination() after 5s", this);
            }
        } catch (InterruptedException e) {
            logger.warn("{}.release() interrupted during executor.awaitTermination()", this);
        }
    }

    @Override public void parallelFor(int from, int to, @Nonnull IntConsumer consumer) {
        if (executor == null)
            throw new IllegalStateException("bind() not called");
        int step = Math.max((to-from) / core, 1);
        int nSteps = (to-from)/step;
        RuntimeException[] ex = {null};
        CountDownLatch latch = new CountDownLatch(nSteps);
        for (int i = 0; i < nSteps; i++) {
            int begin = i * step;
            int end = (i == nSteps-1) ? to : begin+step;
            Runnable runnable = () -> {
                for (int j = begin; j < end; j++) {
                    try {
                        consumer.accept(j);
                    } catch (Throwable t) {
                        String msg = format("Problem at index %d in loop %d...%d", j, from, to);
                        RuntimeException re = new RuntimeException(msg, t);
                        if (ex[0] == null) ex[0] = re;
                        else               ex[0].addSuppressed(re);
                    }
                }
                latch.countDown();
            };
            executor.execute(new FutureTask<>(runnable, null));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting for loop tasks", e);
        }
        if (ex[0] != null)
            throw ex[0];
    }
}
