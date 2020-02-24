package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeBuilder;
import br.ufsc.lapesd.riefederator.description.molecules.MoleculeLink;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
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
    private @Nonnull final Atom core;
    private @LazyInit int atomCount;
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Map<String, Atom>> atomMap
            = new SoftReference<>(null);
    private @LazyInit @SuppressWarnings("Immutable") @Nonnull SoftReference<Index> index
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

        public @Nonnull Term getEdge() {
            return edge;
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
                return stream.filter(t -> t.getSubj().equals(subj));
            if (obj != null)
                return stream.filter(t -> t.getObj().equals(obj));
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
