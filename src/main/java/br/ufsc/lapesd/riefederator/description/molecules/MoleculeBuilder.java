package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MoleculeBuilder {
    private final @Nonnull String name;
    private boolean exclusive = false, closed = false, disjoint = false;
    private @Nonnull Set<MoleculeLink> in = new HashSet<>(), out = new HashSet<>();
    /* Atom names must be unique within a Molecule. This helps enforcing such rule */
    private @Nonnull final Map<String, Atom> name2atom = new HashMap<>();

    public MoleculeBuilder(@Nonnull String name) {
        this.name = name;
    }

    private void checkAtom(@Nonnull Atom atom) {
        Atom old = name2atom.getOrDefault(atom.getName(), null);
        if (old == null) {
            name2atom.put(atom.getName(), atom);
        } else if  (!atom.equals(old)) {
            throw new IllegalArgumentException("Atom names within a molecule must be unique. " +
                    "There already exists an atom named " + atom.getName() + " which " +
                    "is different from the one being added.");
        }
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonExclusive() {return exclusive(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    exclusive() {return exclusive(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder exclusive(boolean value) {
        this.exclusive = value;
        return this;
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonClosed() {return closed(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    closed() {return closed(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder closed(boolean value) {
        this.closed = value;
        return this;
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonDisjoint() {return disjoint(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    disjoint() {return disjoint(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder disjoint(boolean value) {
        this.disjoint = value;
        return this;
    }

    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom) {
        return in(edge, atom, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder inAuthoritative(@Nonnull Term edge, @Nonnull Atom atom) {
        return in(edge, atom, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom, 
                                       boolean authoritative) {
        checkAtom(atom);
        this.in.add(new MoleculeLink(edge, atom, authoritative));
        return this;
    }


    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom) {
        return out(edge, atom, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder outAuthoritative(@Nonnull Term edge, @Nonnull Atom atom) {
        return out(edge, atom, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom,
                                       boolean authoritative) {
        checkAtom(atom);
        this.out.add(new MoleculeLink(edge, atom, authoritative));
        return this;
    }


    @Contract("-> new") public @Nonnull Atom buildAtom() {
        return new Atom(name, exclusive, closed, disjoint, in, out);
    }
    @Contract("-> new") public @Nonnull Molecule build() {
        return new Molecule(buildAtom(), name2atom.size());
    }
}
