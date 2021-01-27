package br.ufsc.lapesd.freqel.description.molecules;

import javax.annotation.Nonnull;

public enum AtomRole {
    INPUT,
    OUTPUT;

    @Override
    public String toString() {
        return super.name();
    }

    public @Nonnull AtomWithRole wrap(@Nonnull Atom atom) {
        return new AtomWithRole(atom.getName(), this);
    }
    public @Nonnull AtomWithRole wrap(@Nonnull String atomName) {
        return new AtomWithRole(atomName, this);
    }
}
