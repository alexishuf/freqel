package br.ufsc.lapesd.freqel.query.modifiers;

import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class Optional implements Modifier {
    public static final Optional IMPLICIT = new Optional(false);
    public static final Optional EXPLICIT = new Optional(true);

    private final boolean explicit;

    protected Optional(boolean explicit) {
        this.explicit = explicit;
    }

    /**
     * An implicit OPTIONAL is a modifier that only has meaning during execution.
     * It happens when a join between two optional components occurs during planning.
     * Since both operands are optional, the join is a full outer join and since both
     * elements where optional, it must retain that treatment in execution of its parent nodes.
     * However, if such join where to be rewritten back into SPARQL, it should not be wrapped
     * inside an OPTIONAL {} clause.
     */
    public boolean isExplicit() {
        return explicit;
    }

    @Override public @Nonnull Capability getCapability() {
        return Capability.OPTIONAL;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Optional && isExplicit() == ((Optional) o).isExplicit();
    }

    @Override
    public int hashCode() {
        return 37*getClass().hashCode() + (explicit ? 17 : 0);
    }

    @Override
    public @Nonnull String toString() {
        return explicit ? "OPTIONAL" : "OPTIONAL[implicit]";
    }
}
