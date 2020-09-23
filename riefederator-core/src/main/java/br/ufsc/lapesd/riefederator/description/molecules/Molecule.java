package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.molecules.tags.AtomTag;
import br.ufsc.lapesd.riefederator.description.molecules.tags.MoleculeLinkTag;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Stream.concat;

@Immutable
public class Molecule {
    private @Nonnull final ImmutableSet<Atom> coreAtoms;
    private @LazyInit int hash;
    private @LazyInit int atomCount;
    private @Nonnull final ImmutableSet<AtomFilter> filters;
    private @Nonnull final ImmutableSetMultimap<String, AtomFilter> atom2Filters;
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<IndexSet<String>> atomNames
            = new SoftReference<>(null);
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Map<String, Atom>> atomMap
            = new SoftReference<>(null);
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Map<String, AtomFilter>> filterMap
            = new SoftReference<>(null);
    private @LazyInit @SuppressWarnings("Immutable") @Nonnull SoftReference<Index> index
            = new SoftReference<>(null);


    /* --- ---- --- Constructor & builder --- --- --- */

    public static @Nonnull MoleculeBuilder builder(@Nonnull String name) {
        return new MoleculeBuilder(name);
    }
    public static @Nonnull MoleculeBuilder builder(@Nonnull Atom atom) {
        return new MoleculeBuilder(atom);
    }

    Molecule(@Nonnull Atom core, int atomCount,
             @Nonnull Collection<AtomFilter> filters) {
        this(Collections.singletonList(core), atomCount, filters);
    }

    Molecule(@Nonnull List<Atom> cores, int atomCount,
             @Nonnull Collection<AtomFilter> filters) {
        this.coreAtoms = ImmutableSet.copyOf(cores);
        this.atomCount = atomCount;
        this.filters = filters instanceof ImmutableSet ? (ImmutableSet<AtomFilter>)filters
                                                       : ImmutableSet.copyOf(filters);
        ImmutableSetMultimap.Builder<String, AtomFilter> b = ImmutableSetMultimap.builder();
        for (AtomFilter filter : filters)
            filter.getAtomNames().forEach(atom -> b.put(atom, filter));
        this.atom2Filters = b.build();
    }

    public Molecule(@Nonnull Atom core) {
        this(core, -1, Collections.emptySet());
    }

    /**
     * Create a builder that already contains this whole {@link Molecule} instance.
     *
     * @return a <code>builder</code> such that <code>builder.build().equals(this)</code>.
     */
    public @Nonnull MoleculeBuilder toBuilder() {
        MoleculeBuilder b = builder(getCore().getName());
        boolean first = true;
        for (Atom core : getCores()) {
            if (first) first = false;
            else       b.startNewCore(core.getName());
            b.exclusive(core.isExclusive()).closed(core.isClosed()).disjoint(core.isDisjoint());
            core.getTags().forEach(b::tag);
            for (MoleculeLink link : core.getIn())
                b.in(link.getEdge(), link.getAtom(), link.isAuthoritative(), link.getTags());
            for (MoleculeLink link : core.getOut())
                b.out(link.getEdge(), link.getAtom(), link.isAuthoritative(), link.getTags());
        }
        getFilters().forEach(b::filter);
        return b;
    }

    /* --- --- --- Getters --- --- --- */

    public int getAtomCount() {
        int local = this.atomCount;
        if (local < 0)
            atomCount = local = getAtomMap().size();
        return local;
    }

    public @Nonnull Atom getCore() {
        if (coreAtoms.isEmpty()) throw new NoSuchElementException();
        return coreAtoms.iterator().next();
    }

    public @Nonnull ImmutableSet<Atom> getCores() {
        return coreAtoms;
    }

    public @Nonnull String dump() {
        return dump(StdPrefixDict.DEFAULT);
    }
    public @Nonnull String dump(@Nonnull PrefixDict dict) {
        StringBuilder builder = new StringBuilder(coreAtoms.size() * 256);
        for (Atom core : coreAtoms)
            core.dump(builder, 2, dict);
        return builder.toString();
    }

    public @Nonnull IndexSet<String> getAtomNames() {
        IndexSet<String> strong = atomNames.get();
        if (strong == null) {
            strong = FullIndexSet.fromDistinct(getAtomMap().keySet());
            atomNames = new SoftReference<>(strong);
        }
        return strong;
    }

    public @Nonnull Map<String, Atom> getAtomMap() {
        Map<String, Atom> strong = atomMap.get();
        if (strong == null) {
            strong = new HashMap<>();
            ArrayDeque<Atom> stack = new ArrayDeque<>();
            getCores().forEach(stack::push);
            while (!stack.isEmpty()) {
                Atom a = stack.pop();
                if (strong.containsKey(a.getName())) continue;
                strong.put(a.getName(), a);
                concat(a.getIn().stream(), a.getOut().stream())
                        .forEach(l -> stack.push(l.getAtom()));
            }
            atomMap = new SoftReference<>(strong);
        }
        return strong;
    }
    public @Nullable Atom getAtom(@Nonnull String name) {
        return getAtomMap().get(name);
    }

    public @Nonnull Map<String, AtomFilter> getAtomFilterMap() {
        Map<String, AtomFilter> strong = filterMap.get();
        if (strong == null) {
            strong = new HashMap<>();
            for (AtomFilter filter : filters)
                strong.put(filter.getName(), filter);
            filterMap = new SoftReference<>(strong);
        }
        return strong;
    }

    public @Nullable AtomFilter getAtomFilter(@Nonnull String name) {
        return getAtomFilterMap().get(name);
    }

    public @Nullable MoleculeElement getElement(@Nonnull String name) {
        Atom atom = getAtomMap().get(name);
        //noinspection AssertWithSideEffects
        assert atom == null || !getAtomFilterMap().containsKey(name);
        if (atom != null) return atom;
        return getAtomFilterMap().get(name);
    }

    /* --- --- --- Filters --- --- --- */

    public @Nonnull ImmutableSet<AtomFilter> getFilters() {
        return filters;
    }

    public @Nonnull ImmutableSet<AtomFilter> getFiltersWithAtom(@Nonnull String atomName) {
        return atom2Filters.get(atomName);
    }
    public @Nonnull ImmutableSet<AtomFilter> getFiltersWithAtom(@Nonnull Atom atom) {
        return getFiltersWithAtom(atom.getName());
    }

    /* --- --- --- Index --- --- --- */

    @Immutable
    public class Triple {
        private final @Nonnull String subj, obj;
        private final @Nonnull Term edge;
        private @SuppressWarnings("Immutable") @LazyInit int hash;

        public Triple(@Nonnull String subj, @Nonnull Term edge, @Nonnull String obj) {
            this.subj = subj;
            this.obj = obj;
            this.edge = edge;
        }

        public @Nonnull String getSubj() {
            return subj;
        }

        public @Nonnull String getObj() {
            return obj;
        }

        public @Nonnull Atom getSubjAtom() {
            return getAtomMap().get(subj);
        }

        public @Nonnull Atom getObjAtom() {
            return getAtomMap().get(obj);
        }

        public @Nonnull ImmutableSet<AtomTag> getSubjTags() {
            return getSubjAtom().getTags();
        }

        public @Nonnull ImmutableSet<AtomTag> getObjTags() {
            return getObjAtom().getTags();
        }

        public @Nonnull Term getEdge() {
            return edge;
        }

        public @Nonnull ImmutableSet<MoleculeLinkTag> getEdgeTags() {
            Atom sa = getSubjAtom(), oa = getObjAtom();
            ImmutableSet.Builder<MoleculeLinkTag> b = ImmutableSet.builder();
            for (MoleculeLink link : getSubjAtom().getOut()) {
                if (link.getEdge().equals(edge) && link .getAtom().equals(oa))
                    b.addAll(link.getTags());
            }
            for (MoleculeLink link : getObjAtom().getIn()) {
                if (link.getEdge().equals(edge) && link.getAtom().equals(sa))
                    b.addAll(link.getTags());
            }
            return b.build();
        }

        public @Nonnull List<Object> asList() {
            return Arrays.asList(getSubj(), getEdge(), getObj());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Triple)) return false;
            Triple triple = (Triple) o;
            return subj.equals(triple.subj) &&
                    obj.equals(triple.obj) &&
                    edge.equals(triple.edge);
        }

        @Override
        public int hashCode() {
            if (hash == 0)
                hash = Objects.hash(subj, obj, edge);
            return hash;
        }
    }

    public @Nonnull Triple toTriple(@Nonnull MoleculeLink link, @Nonnull Atom inAtom) {
        assert inAtom.getIn().contains(link);
        return new Triple(link.getAtom().getName(), link.getEdge(), inAtom.getName());
    }

    public @Nonnull Triple toTriple(@Nonnull Atom outAtom, @Nonnull MoleculeLink link) {
        assert outAtom.getOut().contains(link);
        return new Triple(outAtom.getName(), link.getEdge(), link.getAtom().getName());
    }

    @Immutable
    public class Index {
        private final @SuppressWarnings("Immutable") @Nonnull SetMultimap<Term, Triple> edge2triple;
        private final @SuppressWarnings("Immutable") @Nonnull HashSet<Triple> triples;

        public Index() {
            edge2triple = MultimapBuilder.hashKeys().hashSetValues().build();
            triples = new HashSet<>();
            for (Atom atom : getAtomMap().values()) {
                for (MoleculeLink link : atom.getIn())
                    addTriple(link.getAtom(), link.getEdge(), atom);
                for (MoleculeLink link : atom.getOut())
                    addTriple(atom, link.getEdge(), link.getAtom());
            }
        }

        private void addTriple(Atom subj, Term edge, Atom obj) {
            Triple triple = new Triple(subj.getName(), edge, obj.getName());
            triples.add(triple);
            edge2triple.put(edge, triple);
        }

        public @Nonnull Stream<Triple> stream(@Nullable String subj, @Nullable Term edge,
                                              @Nullable String obj) {
            if (subj == null && edge == null && obj == null) return triples.stream();
            Stream<Triple> stream;
            if (edge == null) {
                stream = triples.stream();
            } else {
                if (subj != null && obj != null) {
                    Triple t = new Triple(subj, edge, obj);
                    return triples.contains(t) ? Stream.of(t) : Stream.of();
                }
                stream = edge2triple.get(edge).stream();
            }
            if (subj != null)
                stream = stream.filter(t -> t.getSubj().equals(subj));
            if (obj != null)
                stream = stream.filter(t -> t.getObj().equals(obj));
            return stream;
        }

        public @Nonnull Set<Triple> get(@Nullable String subj, @Nullable Term edge,
                                        @Nullable String obj) {
            if (subj == null && edge == null && obj == null)
                return triples;
            if (edge != null && subj == null && obj == null)
                return edge2triple.get(edge);
            return stream(subj, edge, obj).collect(Collectors.toSet());
        }
    }

    public @Nonnull Index getIndex() {
        Index strong = this.index.get();
        if (strong == null)
            this.index = new SoftReference<>(strong = new Index());
        return strong;
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public @Nonnull String toString() {
        if (getCores().isEmpty())
            return "EMPTY";
        StringBuilder builder = new StringBuilder(256);
        for (Atom core : getCores())
            builder.append(core.toString()).append(", ");
        builder.setLength(builder.length()-2);
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Molecule) && getCores().equals(((Molecule)o).getCores());
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = getCores().hashCode();
        return hash;
    }
}
