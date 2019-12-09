package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;

import javax.annotation.Nonnull;

public class Molecule implements Description {
    private @Nonnull final Atom core;

    public Molecule(@Nonnull Atom core) {
        this.core = core;
    }

    public @Nonnull Atom getCore() {
        return core;
    }
    public @Nonnull String dump() {
        return getCore().dump();
    }
    public @Nonnull String dump(@Nonnull PrefixDict dict) {
        return getCore().dump(dict);
    }

    @Override
    public @Nonnull String toString() {
        return getCore().toString();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Molecule) && core.equals(((Molecule) o).core);
    }

    @Override
    public int hashCode() {
        return core.hashCode();
    }

//    @Override
//    public CQueryMatch match(@Nonnull List<Triple> query) {
//
//    }
}
