package br.ufsc.lapesd.riefederator.federation.concurrent;

import com.google.common.util.concurrent.ForwardingExecutorService;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

public class CommonPoolPlanningExecutorService extends ForwardingExecutorService
        implements PlanningExecutorService {
    public static final @Nonnull CommonPoolPlanningExecutorService INSTANCE
            = new CommonPoolPlanningExecutorService();

    @Override protected ExecutorService delegate() {
        return ForkJoinPool.commonPool();
    }

    @Override public void bind() {
        /* no-op: leave the common pool intact */
    }

    @Override public void release() {
        /* no-op: leave the common pool intact */
    }

    @Override public void parallelFor(int from, int to, @Nonnull IntConsumer consumer) {
        IntStream.range(from, to).parallel().forEach(consumer);
    }
}
