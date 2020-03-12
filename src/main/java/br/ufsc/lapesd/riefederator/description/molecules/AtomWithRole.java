package br.ufsc.lapesd.riefederator.description.molecules;

import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class AtomWithRole {
    private final @Nonnull String atomName;
    private final @Nonnull AtomRole role;

    public AtomWithRole(@Nonnull String atomName, @Nonnull AtomRole role) {
        this.atomName = atomName;
        this.role = role;
    }

    public @Nonnull String getAtomName() {
        return atomName;
    }

    public @Nonnull AtomRole getRole() {
        return role;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomWithRole)) return false;
        AtomWithRole that = (AtomWithRole) o;
        return getAtomName().equals(that.getAtomName()) &&
                getRole() == that.getRole();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAtomName(), getRole());
    }

    @Override
    public String toString() {
        return getAtomName()+"$"+getRole();
    }
}
