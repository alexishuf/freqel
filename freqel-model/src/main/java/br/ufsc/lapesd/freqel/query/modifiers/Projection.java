package br.ufsc.lapesd.freqel.query.modifiers;

import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.join;

@Immutable
public class Projection implements Modifier {
    private final @SuppressWarnings("Immutable") @Nonnull Set<String> varNames;
    private @LazyInit int hash = 0;

    /* ~~~ Constructor & builder ~~~ */

    public Projection(@Nonnull Set<String> varNames) {
        this.varNames = varNames;
    }

    public static @Nonnull Projection of(String... names) {
        Set<String> set = Sets.newHashSetWithExpectedSize(names.length);
        Collections.addAll(set, names);
        return new Projection(Collections.unmodifiableSet(set));
    }

    public static @Nonnull Projection of(@Nonnull Collection<String> names) {
        Set<String> set = names instanceof Set ? (Set<String>) names : new HashSet<>(names);
        return new Projection(Collections.unmodifiableSet(set));
    }

    /* ~~~ actual methods ~~~ */

    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public @Nonnull Capability getCapability() {
        return Capability.PROJECTION;
    }

    /* ~~~ Object-ish methods ~~~ */

    @Override
    public @Nonnull String toString() {
        return "π("+join(", ", getVarNames())+")";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Projection && ((Projection)o).getVarNames().equals(getVarNames());
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = getVarNames().hashCode();
        return hash;
    }
}
