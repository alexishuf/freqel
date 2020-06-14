package br.ufsc.lapesd.riefederator.federation.performance;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeMetric;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SimpleSamplerRegistry implements SamplerRegistry {
    private static final ThreadLocal<Map<TimeMetric, TimeSampler>> samplerMap
            = ThreadLocal.withInitial(HashMap::new);

    @Override
    public @Nullable TimeSampler getCurrentThreadSampler(@Nonnull TimeMetric metric) {
        return samplerMap.get().get(metric);
    }

    @Override
    public @Nonnull Collection<TimeSampler> getCurrentThreadSamplers() {
        return samplerMap.get().values();
    }

    @Override
    public @Nonnull TimeSampler createSampler(@Nonnull TimeMetric metric,
                                              @Nonnull PerformanceListener listener) {
        TimeSampler sampler = samplerMap.get().get(metric);
        if (sampler == null) {
            sampler = new TimeSampler(listener, metric).stopContaining();
            samplerMap.get().put(metric, sampler);
        }
        return sampler;
    }

    @Override
    public boolean removeCurrentThreadSampler(@Nonnull TimeSampler sampler) {
        return samplerMap.get().remove(sampler.getMetric(), sampler);
    }
}
