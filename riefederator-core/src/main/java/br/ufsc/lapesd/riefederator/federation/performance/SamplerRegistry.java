package br.ufsc.lapesd.riefederator.federation.performance;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeMetric;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

public interface SamplerRegistry {
    /**
     * Gets the {@link TimeSampler} for metric name active at this thread for this listener.
     *
     * @param metric the metric
     * @return The current {@link TimeSampler} or null.
     */
    @Nullable TimeSampler getCurrentThreadSampler(@Nonnull TimeMetric metric);

    /**
     * Gets the collection of active {@link TimeSampler}s in the given listener at this thread.
     *
     * @return a distinct collection of {@link TimeSampler}s. Some may be stop()ed.
     */
    @Nonnull Collection<TimeSampler> getCurrentThreadSamplers();

    /** Gets this thread's {@link TimeSampler} at listener for metric name or creates a
     * new {@link TimeSampler} and make it the thread sampler until it is closed.
     *
     * @param metric the metric
     * @param listener {@link PerformanceListener} where to save the measure
     * @return a new {@link TimeSampler} or the current active in this thread.
     */
    @Nonnull TimeSampler createSampler(@Nonnull TimeMetric metric,
                                       @Nonnull PerformanceListener listener);

    /**
     * Remove the given sampler from the current thread.
     *
     * @param sampler {@link TimeSampler} to remove
     * @return true iff the sampler was in fact removed, false otherwise.
     */
    @CanIgnoreReturnValue
    boolean removeCurrentThreadSampler(@Nonnull TimeSampler sampler);
}
