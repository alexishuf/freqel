package br.ufsc.lapesd.freqel.federation.concurrent;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutorService;
import java.util.function.IntConsumer;

public interface PlanningExecutorService extends ExecutorService {
    void bind();
    void release();
    void parallelFor(int from, int to, @Nonnull IntConsumer consumer);
}
