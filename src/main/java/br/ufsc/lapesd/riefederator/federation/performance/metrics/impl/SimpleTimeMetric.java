package br.ufsc.lapesd.riefederator.federation.performance.metrics.impl;

import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metric;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeMetric;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class SimpleTimeMetric extends SimpleMetric<Double> implements TimeMetric {
    private final @Nonnull  TimeUnit timeUnit;

    public SimpleTimeMetric(@Nonnull String name) {
        this(name, false, ImmutableSet.of(), ImmutableSet.of(), TimeUnit.MILLISECONDS);
    }

    public SimpleTimeMetric(@Nonnull String name, boolean singleValued,
                            @Nonnull ImmutableSet<String> contained,
                            @Nonnull ImmutableSet<String> containedBy, @Nonnull TimeUnit timeUnit) {
        super(name, singleValued, contained, containedBy, Double.class);
        this.timeUnit = timeUnit;
    }

    public static @Nonnull Builder builder(@Nonnull String name) {
        return new Builder(name);
    }

    public static class Builder extends SimpleMetric.Builder {
        private @Nonnull TimeUnit timeUnit = TimeUnit.MILLISECONDS;

        public Builder(@Nonnull String name) {
            super(name);
        }

        public @Nonnull Builder setTimeUnit(@Nonnull TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder contains(@Nonnull Collection<String> other) {
            super.contains(other);  return this;
        }
        @Override @CanIgnoreReturnValue
        public @Nonnull Builder contains(@Nonnull String... other) {
            super.contains(other);  return this;
        }
        @Override @CanIgnoreReturnValue
        public @Nonnull Builder contains(@Nonnull Metric<?>... other) {
            super.contains(other);  return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder containedBy(@Nonnull Collection<String> other) {
            super.containedBy(other);  return this;
        }
        @Override @CanIgnoreReturnValue
        public @Nonnull Builder containedBy(@Nonnull String... other) {
            super.containedBy(other);  return this;
        }
        @Override @CanIgnoreReturnValue
        public @Nonnull Builder containedBy(@Nonnull Metric<?>... other) {
            super.containedBy(other);  return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder setSingleValued(boolean value) {
            super.setSingleValued(value);  return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder setSingleValued() {
            super.setSingleValued();  return this;
        }

        @Override
        public @Nonnull SimpleTimeMetric create() {
            return new SimpleTimeMetric(name,  singleValued, ImmutableSet.copyOf(contained),
                                        ImmutableSet.copyOf(containedBy), timeUnit);
        }
    }

    @Override
    public @Nonnull TimeUnit getTimeUnit() {
        return timeUnit;
    }
}
