package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.join;

@Immutable
public class Projection implements Modifier {
    private final @Nonnull ImmutableSet<String> varNames;
    private final boolean required;
    private @LazyInit int hash = 0;

    /* ~~~ Constructor & builder ~~~ */

    public Projection(@Nonnull ImmutableSet<String> varNames, boolean required) {
        this.varNames = varNames;
        this.required = required;
    }

    public static class Builder {
        private HashSet<String> set;
        private boolean required = false;

        public Builder(int expected) {
            set = new HashSet<>(expected);
        }

        public @Nonnull Set<String> getMutableSet() {
            return set;
        }

        public @Contract("_ -> this") @Nonnull Builder required(boolean value) {
            this.required = value;
            return this;
        }
        public @Contract("-> this") @Nonnull Builder required() { return required(true ); }
        public @Contract("-> this") @Nonnull Builder  advised() { return required(false); }

        public @Contract("_ -> this") @Nonnull Builder add(@Nonnull String string) {
            set.add(string);
            return this;
        }

        public @Contract("-> new") @Nonnull Projection build() {
            return new Projection(ImmutableSet.copyOf(set), required);
        }
    }

    public static @Nonnull Builder builder() {
        return new Builder(8);
    }
    public static @Nonnull Builder builder(int expected) {
        return new Builder(expected);
    }

    public static @Nonnull Projection required(String... names) {
        Builder b = builder(names.length).required();
        for (String name : names) b.add(name);
        return b.build();
    }
    public static @Nonnull Projection advised(String... names) {
        Builder b = builder(names.length).advised();
        for (String name : names) b.add(name);
        return b.build();
    }

    public static @Nonnull Projection required(@Nonnull Collection<String> names) {
        return new Projection(ImmutableSet.copyOf(names), true);
    }
    public static @Nonnull Projection advised(@Nonnull Collection<String> names) {
        return new Projection(ImmutableSet.copyOf(names), false);
    }

    /* ~~~ actual methods ~~~ */

    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.PROJECTION;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    /* ~~~ Object-ish methods ~~~ */

    @Override
    public @Nonnull String toString() {
        return String.format("π[%s](%s)",
                isRequired() ? "required" : "advised", join(", ", getVarNames()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Projection)) return false;
        Projection that = (Projection) o;
        return isRequired() == that.isRequired() &&
                hash == that.hash &&
                getVarNames().equals(that.getVarNames());
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = Objects.hash(getVarNames(), isRequired());
        return hash;
    }
}
