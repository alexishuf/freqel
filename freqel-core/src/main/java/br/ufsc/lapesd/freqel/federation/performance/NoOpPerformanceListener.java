package br.ufsc.lapesd.freqel.federation.performance;


import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metric;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.Collection;
import java.util.Collections;

public class NoOpPerformanceListener implements PerformanceListener {
    public static final @Nonnull NoOpPerformanceListener INSTANCE = new NoOpPerformanceListener();
    public static final @Nonnull SamplerRegistry registry = new SimpleSamplerRegistry();

    public static class SingletonProvider implements Provider<PerformanceListener> {
        @Override
        public PerformanceListener get() {
            return INSTANCE;
        }
    }

    @Override
    public <T> void sample(@Nonnull Metric<T> metric, @Nonnull T value) {
        /* pass */
    }

    @Override
    public void sync() {
        /* pass */
    }

    @Override
    public @Nonnull <T> Collection<T> getValues(@Nonnull Metric<T> metric) {
        return Collections.emptyList();
    }

    @Override
    public @Nullable <T> T getValue(@Nonnull Metric<T> metric) {
        return null;
    }

    @Override
    public @Nonnull SamplerRegistry getSamplerRegistry() {
        return registry;
    }

    @Override
    public void clear() {
        /* pass */
    }

    @Override
    public void close() {

    }
}
