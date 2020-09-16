package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.molecules.tags.AtomTag;
import br.ufsc.lapesd.riefederator.description.molecules.tags.MoleculeLinkTag;
import br.ufsc.lapesd.riefederator.model.term.Term;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.setMinus;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public class MoleculeBuilder {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(MoleculeBuilder.class);

    private @Nonnull Atom currentAtom;
    private @Nonnull final List<Atom> cores = new ArrayList<>();
    /* Atom names must be unique within a Molecule. This helps enforcing such rule */
    private @Nonnull final Map<String, Atom> name2atom = new HashMap<>();
    private @Nonnull final Set<AtomFilter> filterSet = new HashSet<>();

    public MoleculeBuilder(@Nonnull String name) {
        this.currentAtom = Atom.createMutable(name);
        name2atom.put(name, currentAtom);
    }

    public MoleculeBuilder(@Nonnull Atom atom) {
        currentAtom = new Atom(atom);
        registerAtoms(atom);
    }

    private void registerAtoms(@Nonnull Atom atom) {
        Atom old = name2atom.put(atom.getName(), atom);
        assert old == null || old.equals(atom);
        Stream.concat(atom.getIn().stream(), atom.getOut().stream())
                .map(MoleculeLink::getAtom).forEach(this::registerAtoms);
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

    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder startNewCore(@Nonnull String name) {
        buildCore();
        currentAtom = name2atom.computeIfAbsent(name, Atom::createMutable);
        return this;
    }

    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder startNewCore(@Nonnull Atom atom) {
        buildCore();
        currentAtom = new Atom(atom);
        registerAtoms(atom);
        return this;
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonExclusive() {return exclusive(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    exclusive() {return exclusive(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder exclusive(boolean value) {
        currentAtom.setExclusive(value);
        return this;
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonClosed() {return closed(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    closed() {return closed(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder closed(boolean value) {
        currentAtom.setClosed(value);
        return this;
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonDisjoint() {return disjoint(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    disjoint() {return disjoint(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder disjoint(boolean value) {
        currentAtom.setDisjoint(value);
        return this;
    }

    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder tag(@Nonnull String atomName,
                                        @Nonnull Collection<? extends AtomTag> tags) {
        Atom atom = this.name2atom.computeIfAbsent(atomName, Atom::createMutable);
        tags.forEach(atom::addTag);
        return this;
    }
    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder tag(@Nonnull String atomName, @Nonnull AtomTag... tags) {
        Atom atom = this.name2atom.computeIfAbsent(atomName, Atom::createMutable);
        for (AtomTag tag : tags)
            atom.addTag(tag);
        return this;
    }

    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder tag(@Nonnull AtomTag tag) {
        this.currentAtom.addTag(tag);
        return this;
    }

    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder add(@Nonnull Atom atom) {
        checkAtom(atom);
        return this;
    }

    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom) {
        return in(edge, atom, false, emptyList());
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder inAuthoritative(@Nonnull Term edge, @Nonnull Atom atom) {
        return in(edge, atom, true, emptyList());
    }
    @Contract("_, _, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom, 
                                       boolean authoritative,
                                       @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        checkAtom(atom);
        currentAtom.addIn(new MoleculeLink(edge, atom, authoritative, tags));
        return this;
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom,
                                       boolean authoritative) {
        return in(edge, atom, authoritative, emptySet());
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom,
                                       Collection<? extends MoleculeLinkTag> tags) {
        return in(edge, atom, false, tags);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom,
                                       MoleculeLinkTag... tags) {
        return in(edge, atom, false, Arrays.asList(tags));
    }
    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName) {
        return in(edge, atomName, false);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName,
                                       @Nonnull MoleculeLinkTag... tags) {
        return in(edge, atomName, Arrays.asList(tags));
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName,
                                       @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        return in(edge, atomName, false, tags);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder inAuthoritative(@Nonnull Term edge, @Nonnull String atomName) {
        return in(edge, atomName, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName,
                                       boolean authoritative) {
        return in(edge, atomName, authoritative, emptyList());
    }
    @Contract("_, _, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull String atomName,
                                       boolean authoritative,
                                       @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        Atom atom = name2atom.computeIfAbsent(atomName, Atom::createMutable);
        return in(edge, atom, authoritative, tags);
    }


    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom) {
        return out(edge, atom, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder outAuthoritative(@Nonnull Term edge, @Nonnull Atom atom) {
        return out(edge, atom, true);
    }
    @Contract("_, _, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom,
                                        boolean authoritative,
                                        @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        checkAtom(atom);
        currentAtom.addOut(new MoleculeLink(edge, atom, authoritative, tags));
        return this;
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom,
                                        @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        return out(edge, atom, false, tags);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom,
                                        @Nonnull MoleculeLinkTag... tags) {
        return out(edge, atom, false, Arrays.asList(tags));
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom,
                                        boolean authoritative) {
        return out(edge, atom, authoritative, emptyList());
    }
    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName) {
        return out(edge, atomName, false);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName,
                                        @Nonnull MoleculeLinkTag... tags) {
        return out(edge, atomName, Arrays.asList(tags));
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName,
                                        @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        return out(edge, name2atom.computeIfAbsent(atomName, Atom::createMutable), tags);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder outAuthoritative(@Nonnull Term edge, @Nonnull String atomName) {
        return out(edge, atomName, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName,
                                        boolean authoritative) {
        return out(edge, atomName, authoritative, emptyList());
    }
    @Contract("_, _, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull String atomName,
                                        boolean authoritative, Collection<MoleculeLinkTag> tags) {
        Atom atom = name2atom.computeIfAbsent(atomName, Atom::createMutable);
        return out(edge, atom, authoritative, tags);
    }

    public @Nonnull MoleculeBuilder filter(@Nonnull AtomFilter filter) {
        if (!name2atom.keySet().containsAll(filter.getAtomNames())) {
            throw new IllegalArgumentException("Some atoms mentioned by filter are missing " +
                    "from this builder: " + setMinus(filter.getAtomNames(), name2atom.keySet()));
        }
        filterSet.add(filter);
        return this;
    }

    private @Nonnull Atom buildCore() {
        cores.add(currentAtom);
        assert name2atom.containsKey(currentAtom.getName());
        return currentAtom;
    }
    @Contract("-> new") public @Nonnull Atom buildAtom() {
        if (!filterSet.isEmpty())
            logger.warn("buildAtom() will discard filters: {}", filterSet);
        return buildCore();
    }

    @Contract("-> new") public @Nonnull Molecule build() {
        buildCore();
        cores.forEach(Atom::freeze);
        return new Molecule(cores, name2atom.size(), filterSet);
    }
}
