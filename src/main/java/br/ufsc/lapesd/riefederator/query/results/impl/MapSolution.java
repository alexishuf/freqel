package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class MapSolution extends AbstractSolution {
    private final @Nonnull Map<String, Term> map;
    public static final @Nonnull MapSolution EMPTY = new MapSolution(Collections.emptyMap());

    public MapSolution(@Nonnull Map<String, Term> map) {
        this.map = map;
    }
    public MapSolution() {
        this(new HashMap<>());
    }
    public @Nonnull Map<String, Term> getMap() {
        return map;
    }

    /* ~~~ builder and static method factories ~~~ */

    @NotThreadSafe
    public static class Builder {
        private @Nullable HashMap<String, Term> map = new HashMap<>();

        @Contract("_, _ -> this")
        public @Nonnull Builder put(@Nonnull String name, @Nullable Term term) {
            Preconditions.checkState(map != null, "Closed builder");
            map.put(name, term);
            return this;
        }
        @Contract("_, _ -> this")
        public @Nonnull Builder put(@Nonnull Var var, @Nullable Term term) {
            return put(var.getName(), term);
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

    @Contract("_ -> new")
    public static @Nonnull Builder builder(@Nonnull Solution other) {
        Builder builder = new Builder();
        other.forEach(builder::put);
        return builder;
    }

    @Contract("_, _ -> new")
    public static @Nonnull MapSolution build(@Nonnull String name, @Nullable Term term) {
        return builder().put(name, term).build();
    }
    @Contract("_, _ -> new")
    public static @Nonnull MapSolution build(@Nonnull Var var, @Nonnull Term term) {
        return builder().put(var, term).build();
    }


    /* ~~~ method overrides / implementations ~~~ */

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
    public @Nonnull Set<String> getVarNames() {
        return map.keySet();
    }

    @Override
    public boolean equals(@Nullable Object o) {
        return ((o instanceof MapSolution) && map.equals(((MapSolution) o).map)) || super.equals(o);
    }
}
