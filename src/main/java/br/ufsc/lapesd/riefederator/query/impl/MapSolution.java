package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class MapSolution extends AbstractSolution {
    private final @Nonnull Map<String, Term> map;

    public MapSolution(@Nonnull Map<String, Term> map) {
        this.map = map;
    }

    public MapSolution() {
        this(new HashMap<>());
    }

    @NotThreadSafe
    public static class Builder {
        private @Nullable HashMap<String, Term> map = new HashMap<>();

        @Contract("_, _ -> this")
        public @Nonnull Builder put(@Nonnull String name, @Nonnull Term term) {
            Preconditions.checkState(map != null, "Closed builder");
            map.put(name, term);
            return this;
        }

        public @WillClose @Nonnull MapSolution build() {
            Preconditions.checkState(map != null, "Closed builder");
            HashMap<String, Term> old = this.map;
            map = null;
            return new MapSolution(old);
        }
        public @WillClose @Nonnull MapSolution buildUnmodifiable() {
            Preconditions.checkState(map != null, "Closed builder");
            HashMap<String, Term> old = this.map;
            this.map = null;
            return new MapSolution(Collections.unmodifiableMap(old));
        }
        public @Nonnull MapSolution buildAndContinue() {
            Preconditions.checkState(map != null, "Closed builder");
            return new MapSolution(new HashMap<>(map));
        }
    }

    @Contract("-> new")
    public static @Nonnull Builder builder() {
        return new Builder();
    }

    @Contract("_, _ -> new")
    public static @Nonnull MapSolution build(@Nonnull String name, @Nonnull Term term) {
        return builder().put(name, term).build();
    }

    public @Nonnull Map<String, Term> getMap() {
        return map;
    }

    @Override
    @Contract(value = "_, !null -> !null", pure = true)
    public Term get(@Nonnull String varName, Term fallback) {
        return map.getOrDefault(varName, fallback);
    }

    @Override
    public void forEach(@Nonnull BiConsumer<String, Term> consumer) {
        map.forEach(consumer);
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == null)
            return false;
        else if (obj instanceof MapSolution)
            return map.equals(((MapSolution) obj).map);
        else
            return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }
}
