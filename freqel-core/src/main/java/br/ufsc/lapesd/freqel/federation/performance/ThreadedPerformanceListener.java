package br.ufsc.lapesd.freqel.federation.performance;

import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metric;
import com.google.common.collect.LinkedListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link PerformanceListener} that stores everything forever. A thread is
 * used to reduce contention in {@link PerformanceListener#sample(Metric, Object)}
 *
 * Do not use on production!
 */
public class ThreadedPerformanceListener implements PerformanceListener {
    private final @Nonnull Logger logger
            = LoggerFactory.getLogger(ThreadedPerformanceListener.class);
    private final @Nonnull LinkedListMultimap<String, Object> store = LinkedListMultimap.create();
    private final @Nonnull SamplerRegistry samplerRegistry = new SimpleSamplerRegistry();
    private final @Nonnull AtomicInteger age = new AtomicInteger(0);
    private  int syncAge = 0;
    private final @Nonnull ExecutorService executorService;

    public ThreadedPerformanceListener() {
        executorService = new ThreadPoolExecutor(1, 1, 30,
                                                 TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                                                 new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public <T> void sample(@Nonnull Metric<T> metric, @Nonnull T value) {
        age.incrementAndGet();
        executorService.execute(() -> storeSample(metric, value));
    }

    private synchronized  <T> void storeSample(Metric<T> metric, T value) {
        if (metric.isSingleValued())
            store.removeAll(metric.getName());
        store.put(metric.getName(), value);
    }

    @Override
    public void sync() {
        if (!executorService.isShutdown() && age.get() != syncAge) {
            syncAge = age.get();
            try {
                executorService.submit(() -> {}).get();
            } catch (InterruptedException e) {
                logger.error("Interrupted during sync()! Will return with possibly pending results");
            } catch (ExecutionException e) {
                throw new RuntimeException("Unexpected exception", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nonnull <T> List<T> getValues(@Nonnull Metric<T> metric) {
//        sync();
        Class<?> vClass = metric.getValueClass();
        List<T> list = new ArrayList<>();
        synchronized (this) {
            for (Object v : store.get(metric.getName())) {
                if (v == null) {
                    list.add(null);
                } else if (vClass.isAssignableFrom(v.getClass())) {
                    list.add((T) v);
                } else {
                    logger.error("Value {} of class {} unsexpected since metric {} expects class {}",
                                 v, v.getClass(), metric, vClass);
                }
            }
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    @Override
    public @Nullable <T> T getValue(@Nonnull Metric<T> metric) {
//        sync();
        Class<?> vClass = metric.getValueClass();
        synchronized (this) {
            List<Object> list = store.get(metric.getName());
            for (ListIterator<Object> it = list.listIterator(list.size()); it.hasPrevious(); ) {
                Object v = it.previous();
                if (v == null || vClass.isAssignableFrom(v.getClass()))
                    return (T) v;
            }
        }
        return null;
    }

    @Override
    public @Nonnull SamplerRegistry getSamplerRegistry() {
        return samplerRegistry;
    }

    @Override
    public void clear() {
        sync();
        synchronized (this) {
            store.clear();
        }
    }

    @Override
    public synchronized void close() {
        executorService.shutdown();
//        try {
//            executorService.awaitTermination(10, TimeUnit.SECONDS);
//        } catch (InterruptedException ignored) {
//            Thread.currentThread().interrupt();
//        }
    }
}
