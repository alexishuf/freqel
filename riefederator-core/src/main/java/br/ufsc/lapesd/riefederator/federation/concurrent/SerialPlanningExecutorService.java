package br.ufsc.lapesd.riefederator.federation.concurrent;

import com.google.common.base.Stopwatch;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

public class SerialPlanningExecutorService extends AbstractExecutorService implements PlanningExecutorService {
    public static final @Nonnull SerialPlanningExecutorService INSTANCE =
            new SerialPlanningExecutorService();
    private boolean shutdown;

    @Override public void bind() { }

    @Override public void release() { }

    @Override public void parallelFor(int from, int to, @Nonnull IntConsumer consumer) {
        for (int i = from; i < to; i++)
            consumer.accept(i);
    }

    /*  --- ---- --- implement AbstractExecutorService --- --- ---  */

    @Override public synchronized void shutdown() {
        shutdown = true;
        notifyAll();
    }

    @Override public @Nonnull List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override public boolean isShutdown() {
        return shutdown;
    }

    @Override public boolean isTerminated() {
        return shutdown;
    }

    @Override
    public synchronized boolean awaitTermination(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        long timeoutMillis = unit.toNanos(timeout);
        while (!shutdown && sw.elapsed(TimeUnit.MILLISECONDS) <= timeoutMillis)
            wait(Math.max(0, timeoutMillis - sw.elapsed(TimeUnit.MILLISECONDS)));
        return shutdown;
    }

    @Override public void execute(@Nonnull Runnable command) {
        command.run();
    }
}
