package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

import static java.lang.String.join;

@Immutable
public class Projection implements Modifier {
    private final @Nonnull ImmutableSet<String> varNames;
    private @LazyInit int hash = 0;

    /* ~~~ Constructor & builder ~~~ */

    public Projection(@Nonnull Set<String> varNames) {
        this.varNames = ImmutableSet.copyOf(varNames);
    }

    public static @Nonnull Projection of(String... names) {
        //noinspection UnstableApiUsage
        ImmutableSet.Builder<String> b = ImmutableSet.builderWithExpectedSize(names.length);
        for (String name : names)
            b.add(name);

        return new Projection(b.build());
    }

    public static @Nonnull Projection of(@Nonnull Collection<String> names) {
        return new Projection(ImmutableSet.copyOf(names));
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
