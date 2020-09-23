package br.ufsc.lapesd.riefederator.linkedator.strategies.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeElement;
import br.ufsc.lapesd.riefederator.query.SimplePath;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

@Immutable
public class AtomSignature {
    private final @Nonnull Molecule molecule;
    /** Reference atom. This is the result while the keys of atomPaths are inputs */
    private final @Nonnull String anchor;
    /** Path from the core atom to the anchor atom */
    private final @Nonnull SimplePath anchorPath;
    /** Subset of atomPaths's keys that are required atoms */
    private final @Nonnull ImmutableSet<String> requiredAtoms;
    /** For evey atom, the SimplePath (in predicates) from anchor to the Atom. */
    private final @Nonnull ImmutableMap<String, SimplePath> atomPaths;

    public AtomSignature(@Nonnull Molecule molecule, @Nonnull String anchor,
                         @Nonnull SimplePath anchorPath,
                         @Nonnull Set<String> requiredAtoms,
                         @Nonnull Map<String, SimplePath> atomPaths) {
        this.molecule = molecule;
        this.anchor = anchor;
        this.anchorPath = anchorPath;
        this.requiredAtoms = ImmutableSet.copyOf(requiredAtoms);
        this.atomPaths = ImmutableMap.copyOf(atomPaths);
    }

    public @Nonnull Molecule getMolecule() {
        return molecule;
    }

    public @Nonnull String getAnchor() {
        return anchor;
    }

    public @Nonnull SimplePath getAnchorPath() {
        return anchorPath;
    }

    public @Nonnull ImmutableSet<String> getRequiredAtoms() {
        return requiredAtoms;
    }

    public @Nonnull Set<String> getAtoms() {
        return atomPaths.keySet();
    }

    public @Nonnull ImmutableMap<String, SimplePath> getAtomPaths() {
        return atomPaths;
    }

    /**
     * A signature is directly anchored iff all atoms are connected to the anchor by a single predicate
     */
    public boolean isDirectAnchor() {
        return atomPaths.values().stream().allMatch(SimplePath::isSingle);
    }

    public static @Nonnull Set<AtomSignature>
    createOutputSignatures(@Nonnull AtomSignature inSig, @Nonnull Molecule mol) {
        Set<String> atoms = new HashSet<>();
        for (String atom : inSig.getAtoms()) {
            if (mol.getAtom(atom) != null)
                atoms.add(atom);
            else if (inSig.requiredAtoms.contains(atom))
                return emptySet(); // unsatisfiable required input
        }
        if (atoms.isEmpty())
            return emptySet();
        return createSignatures(mol, atoms, inSig.getRequiredAtoms());
    }

    public static @Nonnull Set<AtomSignature> createInputSignatures(@Nonnull APIMolecule apiMol) {
        Set<String> atoms = new HashSet<>();
        ImmutableSet.Builder<String> requiredAtomsBuilder = ImmutableSet.builder();
        Molecule mol = apiMol.getMolecule();
        for (Map.Entry<String, String> e : apiMol.getElement2Input().entrySet()) {
            MoleculeElement element = mol.getElement(e.getKey());
            if (element instanceof Atom) {
                String atom = element.getName();
                atoms.add(atom);
                if (apiMol.getExecutor().getRequiredInputs().contains(e.getValue()))
                    requiredAtomsBuilder.add(atom);
            } else {
                return emptySet(); //not supported (yet)
            }
        }
        return createSignatures(mol, atoms, requiredAtomsBuilder.build());
    }

    private static @Nonnull Set<AtomSignature>
    createSignatures(@Nonnull Molecule mol, @Nonnull Set<String> atoms,
                     @Nonnull ImmutableSet<String> requiredAtoms) {
        Multimap<String, AtomPath> pathsFromCore = findPathsFromCore(mol, atoms);
        IndexSet<String> allAtoms = mol.getAtomNames();
        IndexSubset<String> commonAncestors = allAtoms.fullSubset();
        for (String a : atoms) {
            IndexSubset<String> subset = allAtoms.emptySubset();
            pathsFromCore.get(a).forEach(l -> subset.addAll(l.subList(0, l.size() - 1)));
            commonAncestors.retainAll(subset);
        }
        assert commonAncestors.contains(mol.getCore().getName());

        Set<AtomSignature> result = new HashSet<>();
        anchor_loop:
        for (String anchor : commonAncestors) {
            Atom anchorAtom = mol.getAtom(anchor);
            assert anchorAtom != null;

            ImmutableMap.Builder<String, SimplePath> pathsBuilder;
            //noinspection UnstableApiUsage
            pathsBuilder = ImmutableMap.builderWithExpectedSize(atoms.size());
            for (Map.Entry<String, AtomPath> e : pathsFromCore.entries()) {
                AtomPath subPath = e.getValue().startingAt(anchor);
                assert subPath != null;
                SimplePath simple = subPath.toSimplePath(mol);
                if (simple == null) { //ambiguous
                    if (requiredAtoms.contains(e.getKey()))
                        continue anchor_loop; // give up on anchor
                } else {
                    pathsBuilder.put(e.getKey(), simple);
                }
            }
            Multimap<String, AtomPath> mm = findPathsFromCore(mol, singleton(anchor));
            for (AtomPath atomPath : mm.get(anchor)) {
                SimplePath simplePath = atomPath.toSimplePath(mol);
                if (simplePath != null)
                    result.add(new AtomSignature(mol, anchor, simplePath, requiredAtoms, pathsBuilder.build()));
            }
        }
        return result;
    }

    private static class State {
        final @Nonnull String atom;
        final @Nullable State ancestor;

        public State(@Nonnull String atom, @Nonnull State ancestor) {
            this.atom = atom;
            this.ancestor = ancestor;
        }

        public State(@Nonnull String atom) {
            this.atom = atom;
            this.ancestor = null;
        }

        public @Nonnull State descend(@Nonnull String atom) {
            return new State(atom, this);
        }

        public boolean hasCycle() {
            Set<String> atoms = new HashSet<>();
            for (State state = this; state != null; state = state.ancestor) {
                if (!atoms.add(state.atom))
                    return true;
            }
            return false;
        }

        public  @Nonnull AtomPath toAtomPath() {
            List<String> list = new ArrayList<>();
            for (State state = this; state != null; state = state.ancestor)
                list.add(state.atom);
            Collections.reverse(list);
            return new AtomPath(list);
        }

        @Override
        public @Nonnull String toString() {
            return "State(" + toAtomPath() + ")";
        }
    }

    @VisibleForTesting
    static @Nonnull Multimap<String, AtomPath>
    findPathsFromCore(@Nonnull Molecule molecule, @Nonnull Set<String> atomNames) {
        Multimap<String, AtomPath> result;
        result = HashMultimap.create(atomNames.size(), 4);

        Molecule.Index idx = molecule.getIndex();
        IndexSubset<String> visited = molecule.getAtomNames().emptySubset();
        ArrayDeque<State> queue = new ArrayDeque<>();
        String coreAtom = molecule.getCore().getName();
        queue.add(new State(coreAtom));
        while (!queue.isEmpty()) {
            State s = queue.removeFirst();
            if (s.hasCycle())
                continue;
            // this check is early to allow the atom being reached from multiple paths
            if (atomNames.contains(s.atom) && !s.atom.equals(coreAtom))
                result.put(s.atom, s.toAtomPath());
            if (!visited.add(s.atom))
                continue; //atom already expanded, no work
            // expand the atom and stack all neighbours
            Stream.concat(
                    idx.stream(null, null, s.atom).map(Molecule.Triple::getSubj),
                    idx.stream(s.atom, null, null).map(Molecule.Triple::getObj)
            ).forEach(a -> queue.add(s.descend(a)));
        }
        if (atomNames.contains(coreAtom))
            result.put(coreAtom, AtomPath.EMPTY);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomSignature)) return false;
        AtomSignature that = (AtomSignature) o;
        return getMolecule().equals(that.getMolecule()) &&
                getAnchor().equals(that.getAnchor()) &&
                getAnchorPath().equals(that.getAnchorPath()) &&
                getRequiredAtoms().equals(that.getRequiredAtoms()) &&
                getAtomPaths().equals(that.getAtomPaths());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMolecule(), getAnchor(), getAnchorPath(), getRequiredAtoms(), getAtomPaths());
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append("AtomSignature{\n  molecule=").append(molecule)
                .append("\n  anchor=").append(anchor)
                .append("\n  requiredAtoms=").append(requiredAtoms)
                .append("\n  atomPaths:");
        for (Map.Entry<String, SimplePath> e : atomPaths.entrySet())
            b.append("\n    ").append(e.getKey()).append('=').append(e.getValue());
        return b.append("\n}").toString();
    }
}
