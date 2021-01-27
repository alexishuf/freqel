package br.ufsc.lapesd.freqel.federation.performance.metrics;

import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

public class TimeSampler implements AutoCloseable {
    private final @Nonnull TimeMetric metric;
    private final @Nonnull PerformanceListener listener;
    private final @Nonnull Stopwatch sw;
    private int stopDepth = 0;
    private List<TimeSampler> stopped = null;

    public TimeSampler(@Nonnull PerformanceListener listener, @Nonnull TimeMetric metric) {
        this.metric = metric;
        this.listener = listener;
        this.sw = Stopwatch.createStarted();
    }

    /**
     * Pause all samplers registered at the same {@link PerformanceListener} in the current
     * thread whose {@link TimeMetric}s contain this metric.
     *
     * If this method is called, subsequent calls to {@link TimeSampler#stop()}
     * and {@link TimeSampler#resume()} will undo and redo the effects of this method.
     *
     * @return this {@link TimeSampler} for chaining.
     */
    public @Nonnull TimeSampler stopContaining() {
        checkState(stopped == null || stopped.isEmpty(),
                   "Already paused containing TimeSamplers");
        if (stopped == null)
            stopped = new ArrayList<>();
        for (TimeSampler old : listener.getSamplerRegistry().getCurrentThreadSamplers()) {
            if (old == this) continue;
            if (!old.getMetric().contains(getMetric()) && !old.stopped.contains(this)) {
                old.stop();
                stopped.add(old);
            }
        }
        return this;
    }

    public @Nonnull TimeMetric getMetric() {
        return metric;
    }

    public boolean isRunning() {
        assert stopDepth > 0 || sw.isRunning();
        return stopDepth == 0;
    }

    @CanIgnoreReturnValue
    public @Nonnull TimeSampler stop() {
        assert stopDepth >= 0;
        ++stopDepth;
        if (stopDepth > 1)
            return this;
        assert stopDepth == 1;
        sw.stop();
        if (stopped != null) {
            stopped.forEach(TimeSampler::resume);
            stopped.clear();
        }
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull TimeSampler resume()  {
        assert stopDepth > 0 : "resume() called with stopDepth="+stopDepth;
        --stopDepth;
        if (stopDepth > 0)
            return this; // do not resume yet
        if (stopped != null) {
            assert stopped.isEmpty() : "During effective (depth=0) resume() the stopped set " +
                    "should be empty. Most likely cause: missed a previous effective stop";
            stopContaining();
        }
        sw.start();
        return this;
    }

    public double getValue() {
        double quotient = TimeUnit.MICROSECONDS.convert(1, getMetric().getTimeUnit());
        return sw.elapsed(TimeUnit.MICROSECONDS) / quotient;
    }

    public @Nonnull TimeSampler save() {
        listener.sample(metric, getValue());
        return this;
    }

    @Override
    public String toString() {
        return "TimeSampler(" + metric + ")@" + getValue() + ' ' + getMetric().getTimeUnit();
    }

    @Override
    public void close() {
        if (isRunning())
            stop();
        save();
        listener.getSamplerRegistry().removeCurrentThreadSampler(this);
    }
}
