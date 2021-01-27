package br.ufsc.lapesd.freqel.federation.performance.metrics;

import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public interface Metric<T> {
    @Nonnull String getName();
    @Nonnull Class<?> getValueClass();
    boolean isSingleValued();

    /**
     * Gets the last value for this metric in the listener, or fallback if there are no values.
     *
     * @param listener the listener where values were stored
     * @param fallback a fallback value if there are no values
     */
    @Contract("_, !null -> !null") T get(@Nonnull PerformanceListener listener, T fallback);

    default @Nullable T get(@Nonnull PerformanceListener listener) {
        return get(listener, null);
    }

    @Nonnull Set<String> getContained();
    @Nonnull Set<String> getContainedBy();
    boolean containsAnything();

    default boolean contains(@Nonnull Metric<?> other) {
        return containsAnything()
                || getContained().contains(other.getName())
                || other.getContainedBy().contains(this.getName());
    }

    default boolean isContainedBy(@Nonnull Metric<?> other) {
        return other.containsAnything()
                || getContainedBy().contains(other.getName())
                || other.getContained().contains(this.getName());
    }
}
