package br.ufsc.lapesd.riefederator.federation.performance.metrics.impl;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metric;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class SimpleMetric<T> extends AbstractMetric<T> {
    private final @Nonnull String name;
    private final boolean singleValued;
    private final @Nonnull ImmutableSet<String> contained, containedBy;
    private final @Nullable Class<T> valueClass;

    public SimpleMetric(@Nonnull String name, boolean singleValued,
                        @Nonnull ImmutableSet<String> contained,
                        @Nonnull ImmutableSet<String> containedBy, @Nullable Class<T> valueClass) {
        this.name = name;
        this.singleValued = singleValued;
        this.contained = contained;
        this.containedBy = containedBy;
        this.valueClass = valueClass;
    }

    public static @Nonnull Builder builder(@Nonnull String name) {
        return new Builder(name);
    }

    public static class Builder {
        protected final @Nonnull String name;
        protected boolean singleValued;
        protected  @Nonnull Set<String> contained = new HashSet<>(), containedBy = new HashSet<>();

        public Builder(@Nonnull String name) {
            this.name = name;
        }

        public @CanIgnoreReturnValue @Nonnull Builder contains(@Nonnull Collection<String> other) {
            contained.addAll(other);
            return this;
        }
        public @CanIgnoreReturnValue @Nonnull Builder contains(@Nonnull String... other) {
            return contains(asList(other));
        }
        public @CanIgnoreReturnValue @Nonnull Builder contains(@Nonnull Metric<?>... other) {
            return contains(Stream.of(other).map(Metric::getName).collect(toList()));
        }

        public @CanIgnoreReturnValue @Nonnull Builder containedBy(@Nonnull Collection<String> other) {
            containedBy.addAll(other);
            return this;
        }
        public @CanIgnoreReturnValue @Nonnull Builder containedBy(@Nonnull String... other) {
            return containedBy(asList(other));
        }
        public @CanIgnoreReturnValue @Nonnull Builder containedBy(@Nonnull Metric<?>... other) {
            return containedBy(Stream.of(other).map(Metric::getName).collect(toList()));
        }


        public @CanIgnoreReturnValue @Nonnull Builder setSingleValued(boolean value) {
            singleValued = value;
            return this;
        }
        public @CanIgnoreReturnValue @Nonnull Builder setSingleValued() {
            return setSingleValued(true);
        }

        public @Nonnull SimpleMetric<?> create() {
            return new SimpleMetric<>(name, singleValued, ImmutableSet.copyOf(contained),
                                      ImmutableSet.copyOf(containedBy), null);
        }

        public @Nonnull <U> SimpleMetric<U> create(@Nonnull Class<U> valueClass) {
            return new SimpleMetric<>(name, singleValued, ImmutableSet.copyOf(contained),
                                       ImmutableSet.copyOf(containedBy), valueClass);
        }
    }

    @Override
    public @Nonnull Class<?> getValueClass() {
        return valueClass == null ? Object.class : valueClass;
    }

    @Override
    public @Nonnull String getName() {
        return name;
    }

    @Override
    public boolean isSingleValued() {
        return singleValued;
    }

    @Override
    public T get(@Nonnull PerformanceListener listener, T fallback) {
        return fallback;
    }

    @Override
    public @Nonnull Set<String> getContained() {
        return contained;
    }

    @Override
    public @Nonnull Set<String> getContainedBy() {
        return containedBy;
    }
}
