package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.federation.performance.SamplerRegistry;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metric;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

public interface PerformanceListener extends AutoCloseable {
    /**
     * Stores a sample of a value. Samples are multi-valued by default.
     *
     * Implementations may impose eviction policies.
     *
     * @param metric type of sample being stored
     * @param value the value to store
     */
    <T> void sample(@Nonnull Metric<T> metric, @Nonnull T value);

    /**
     * Some implementations of {@link PerformanceListener#sample(Metric, Object)} may be
     * asynchronous. For such implementations, this method waits in a barrier so that
     * upon its return all effects of a {@link PerformanceListener#sample(Metric, Object)}
     * call previous to its entry are visible.
     */
    void sync();

    /**
     * Get all values, ordered from oldest to newest
     *
     * @param metric the metric
     */
    @Nonnull <T> Collection<T> getValues(@Nonnull Metric<T> metric);

    /**
     * Get latest value for given metric
     *
     * @param metric the metric
     * @return latest value (which can be null) or null if there is no value.
     */
    @Nullable <T> T getValue(@Nonnull Metric<T> metric);

    default @Nonnull <T> T getValue(@Nonnull Metric<?> metric, @Nonnull T fallback) {
        @SuppressWarnings("unchecked") T value = (T) getValue(metric);
        return value == null ? fallback : value;
    }

    /**
     * Get the sampler registry for this PerformanceListener.
     */
    @Nonnull SamplerRegistry getSamplerRegistry();

    /**
     * Discard all data
     */
    void clear();

    @Override
    void close();
}
