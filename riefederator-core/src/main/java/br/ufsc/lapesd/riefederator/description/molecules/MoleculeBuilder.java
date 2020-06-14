package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.model.term.Term;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.setMinus;
import static com.google.common.base.Preconditions.checkArgument;

public class MoleculeBuilder {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(MoleculeBuilder.class);

    private final @Nonnull String name;
    private boolean exclusive = false, closed = false, disjoint = false;
    private @Nonnull Set<MoleculeLink> in = new HashSet<>(), out = new HashSet<>();
    /* Atom names must be unique within a Molecule. This helps enforcing such rule */
    private @Nonnull final Map<String, Atom> name2atom = new HashMap<>();
    private @Nonnull final Set<AtomFilter> filterSet = new HashSet<>();

    public MoleculeBuilder(@Nonnull String name) {
        this.name = name;
    }

    private void checkAtom(@Nonnull Atom atom) {
        Atom old = name2atom.getOrDefault(atom.getName(), null);
        if (old == null) {
            name2atom.put(atom.getName(), atom);
            atom.streamNeighbors().forEach(this::checkAtom);
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

    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder add(@Nonnull Atom atom) {
        checkAtom(atom);
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
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName) {
        return in(edge, atomName, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder inAuthoritative(@Nonnull Term edge, @Nonnull String atomName) {
        return in(edge, atomName, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName,
                                       boolean authoritative) {
        checkArgument(name2atom.containsKey(atomName),
                "No Atom named "+atomName+" in this molecule so far");
        return in(edge, name2atom.get(atomName), authoritative);
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
    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName) {
        return out(edge, atomName, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder outAuthoritative(@Nonnull Term edge, @Nonnull String atomName) {
        return out(edge, atomName, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName,
                                        boolean authoritative) {
        checkArgument(name2atom.containsKey(atomName),
                "No Atom named "+atomName+" in this molecule so far");
        return out(edge, name2atom.get(atomName), authoritative);
    }

    public @Nonnull MoleculeBuilder filter(@Nonnull AtomFilter filter) {
        Set<String> missing = setMinus(filter.getAtomNames(), name2atom.keySet());
        checkArgument(missing.isEmpty(),
                      "Some atoms mentioned by filter are missing from this builder: "+missing);
        filterSet.add(filter);
        return this;
    }

    @Contract("-> new") public @Nonnull Atom buildAtom() {
        if (!filterSet.isEmpty())
            logger.warn("buildAtom() will discard filters: {}", filterSet);
        return new Atom(name, exclusive, closed, disjoint, in, out);
    }
    @Contract("-> new") public @Nonnull Molecule build() {
        Atom atom = new Atom(name, exclusive, closed, disjoint, in, out);
        return new Molecule(atom, name2atom.size(), filterSet);
    }
}
