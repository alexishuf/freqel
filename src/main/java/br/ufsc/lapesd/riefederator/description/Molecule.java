package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Stream.concat;

@Immutable
public class Molecule {
    private @Nonnull final Atom core;
    private @LazyInit int atomCount;
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Map<String, Atom>> atomMap
            = new SoftReference<>(null);

    public static @Nonnull MoleculeBuilder builder(@Nonnull String name) {
        return new MoleculeBuilder(name);
    }

    public Molecule(@Nonnull Atom core, int atomCount) {
        this(core);
        this.atomCount = atomCount;
    }
    public Molecule(@Nonnull Atom core) {
        this.core = core;
    }

    public int getAtomCount() {
        int local = this.atomCount;
        if (local < 0)
            atomCount = local = getAtomMap().size();
        return local;
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

    public @Nonnull Map<String, Atom> getAtomMap() {
        Map<String, Atom> strong = atomMap.get();
        if (strong == null) {
            strong = new HashMap<>();
            ArrayDeque<Atom> stack = new ArrayDeque<>();
            stack.push(getCore());
            while (!stack.isEmpty()) {
                Atom a = stack.pop();
                if (strong.containsKey(a.getName())) continue;
                strong.put(a.getName(), a);
                concat(a.getIn().stream(), a.getOut().stream())
                        .forEach(l -> stack.push(l.getAtom()));
            }
        }
        return strong;
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
}
