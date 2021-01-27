package br.ufsc.lapesd.freqel.federation.performance.metrics.impl;

import br.ufsc.lapesd.freqel.federation.performance.metrics.Metric;

public abstract class AbstractMetric<T> implements Metric<T> {
    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Metric)) return false;
        return ((Metric<?>)obj).getName().equals(getName());
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }
}
