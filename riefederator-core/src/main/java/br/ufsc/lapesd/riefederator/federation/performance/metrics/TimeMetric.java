package br.ufsc.lapesd.riefederator.federation.performance.metrics;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.SamplerRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

public interface TimeMetric extends Metric<Double> {
    @Nonnull TimeUnit getTimeUnit();

    /**
     * Gets the current open {@link TimeSampler}, for this metric, if any.
     *
     * @param listener {@link PerformanceListener} to which the sampler should be bound
     * @return the sampler or null if there is none
     */
    default @Nullable TimeSampler getThreadSampler(@Nonnull PerformanceListener listener) {
        return listener.getSamplerRegistry().getCurrentThreadSampler(this);
    }

    /**
     * Creates a new {@link TimeSampler} in this thread for this metric at the given listener.
     *
     * This method ensures creation of a new {@link TimeSampler}. Therefore callers of this
     * method have the responsibility of calling its {@link TimeSampler#close()} method.
     *
     * @param listener {@link PerformanceListener} to bound the {@link TimeSampler}.
     * @throws IllegalStateException if there already is a {@link TimeSampler} for this metric
*                                    in this thread at the listener.
     * @return the new {@link TimeSampler}.
     */
    default @Nonnull TimeSampler
    createThreadSampler(@Nonnull PerformanceListener listener) throws IllegalStateException {
        SamplerRegistry r = listener.getSamplerRegistry();
        checkState(r.getCurrentThreadSampler(this) == null,
                   "There is an already active TimeSampler for "+this+" in this thread");
        return r.createSampler(this, listener);
    }
}
