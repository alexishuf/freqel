package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeMatcher;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import static java.util.stream.Stream.concat;

@Immutable
public class Molecule implements Description {
    private @Nonnull final Atom core;
    private final int atomCount;
    private @Nonnull final MoleculeMatcher matcher;

    public static @Nonnull MoleculeBuilder builder(@Nonnull String name) {
        return new MoleculeBuilder(name);
    }

    public Molecule(@Nonnull Atom core, int atomCount) {
        this.core = core;
        this.atomCount = atomCount;
        this.matcher = new MoleculeMatcher(this);
    }
    public Molecule(@Nonnull Atom core) {
        this(core, countAtoms(core));
    }

    private static int countAtoms(@Nonnull Atom atom) {
        Set<Atom> set = new HashSet<>();
        Queue<Atom> queue = new ArrayDeque<>();
        queue.add(atom);
        while (!queue.isEmpty()) {
            Atom a = queue.remove();
            if (!set.add(a)) continue;
            concat(a.getIn().stream(), a.getOut().stream()).forEach(l -> queue.add(l.getAtom()));
        }
        return set.size();
    }

    public int getAtomCount() {
        return atomCount;
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

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        return matcher.match(query);
    }
}
