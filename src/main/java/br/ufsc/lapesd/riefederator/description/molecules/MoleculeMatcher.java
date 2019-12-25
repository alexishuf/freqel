package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticCQueryMatch;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticDescription;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class MoleculeMatcher implements SemanticDescription {
    private final @Nonnull Molecule molecule;
    private final @Nonnull TBoxReasoner reasoner;
    private @Nonnull SoftReference<Index> index = new SoftReference<>(null);

    public MoleculeMatcher(@Nonnull Molecule molecule, @Nonnull TBoxReasoner reasoner) {
        this.molecule = molecule;
        this.reasoner = reasoner;
    }

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        return new State(query, false).matchExclusive().matchNonExclusive().build();
    }

    @Override
    public @Nonnull SemanticCQueryMatch semanticMatch(@Nonnull CQuery query) {
        return new State(query, true).matchExclusive().matchNonExclusive().build();
    }

    @Override
    public @Nonnull String toString() {
        return String.format("MoleculeMatcher2(%s)", molecule.getCore().getName());
    }

    private @Nonnull Index getIndex() {
        Index strong = this.index.get();
        if (strong == null) index = new SoftReference<>(strong = new Index());
        return strong;
    }

    private final static class Link {
        @Nonnull Atom s, o;
        @Nonnull Term p;
        private int hash = 0;

        public Link(@Nonnull Atom s, @Nonnull Term p, @Nonnull Atom o) {
            this.s = s;
            this.p = p;
            this.o = o;
        }

        @Override
        public boolean equals(Object o1) {
            if (this == o1) return true;
            if (o1 == null || getClass() != o1.getClass()) return false;
            Link other = (Link) o1;
            return s.equals(other.s) && p.equals(other.p) && o.equals(other.o);
        }

        @Override
        public int hashCode() {
            if (hash == 0)
                hash = Objects.hash(s, o, p, hash);
            return hash;
        }
    }

    private final static class LinkMatch {
        @Nonnull Link l;
        @Nonnull ImmutablePair<Term, Atom> from;
        int tripleIdx;
        @Nonnull Triple triple;

        public LinkMatch(@Nonnull Link l, @Nonnull ImmutablePair<Term, Atom> from, int tripleIdx,
                         @Nonnull Triple triple) {
            this.l = l;
            this.from = from;
            this.tripleIdx = tripleIdx;
            this.triple = triple;
        }

        public @Nonnull ImmutablePair<Term, Atom> getTo() {
            String atomName = from.right.getName();
            if (l.s.getName().equals(atomName) && triple.getSubject().equals(from.left))
                return ImmutablePair.of(triple.getObject(), l.o);
            else
                return ImmutablePair.of(triple.getSubject(), l.s);
        }
    }

    private class Index {
        private final @Nonnull Map<Term, SetMultimap<String, Link>> map;
        private final @Nonnull Multimap<Term, Link> pred2link;
        private final @Nonnull Multimap<String, Link> subj, obj;
        private final @Nonnull List<Atom> exclusive;
        private final @Nonnull LoadingCache<Term, List<Term>> predicatesInIndex;

        private final int atomCount;

        private Index() {
            atomCount = molecule.getAtomCount();
            int capacity = atomCount * 8;
            map = new HashMap<>(capacity);
            subj = MultimapBuilder.hashKeys(atomCount).hashSetValues().build();
            obj = MultimapBuilder.hashKeys(atomCount).hashSetValues().build();
            pred2link = MultimapBuilder.hashKeys(capacity).arrayListValues().build();
            exclusive = new ArrayList<>(atomCount);
            predicatesInIndex = CacheBuilder.newBuilder().build(new CacheLoader<Term, List<Term>>() {
                @Override
                public List<Term> load(@Nonnull Term key) {
                    return loadIndexedSubProperties(key);
                }
            });

            Queue<Atom> queue = new ArrayDeque<>();
            queue.add(molecule.getCore());
            HashSet<String> visited = new HashSet<>();
            while (!queue.isEmpty()) {
                Atom a = queue.remove();
                if (!visited.add(a.getName()))
                    continue;
                if (a.isExclusive())
                    exclusive.add(a);
                for (MoleculeLink l : a.getIn()) {
                    queue.add(l.getAtom());
                    Link link = new Link(l.getAtom(), l.getEdge(), a);
                    if (!a.isExclusive())
                        pred2link.put(l.getEdge(), link);
                    getAtom2Link(l.getEdge()).put(a.getName(), link);
                    subj.put(a.getName(), link);
                }
                for (MoleculeLink l : a.getOut()) {
                    queue.add(l.getAtom());
                    Link link = new Link(a, l.getEdge(), l.getAtom());
                    if (!a.isExclusive())
                        pred2link.put(l.getEdge(), link);
                    getAtom2Link(l.getEdge()).put(a.getName(), link);
                    obj.put(a.getName(), link);
                }
            }
        }

        private List<Term> loadIndexedSubProperties(@Nonnull Term predicate)  {
            Preconditions.checkArgument(predicate.isGround());
            return Stream.concat(reasoner.subProperties(predicate), Stream.of(predicate))
                    .filter(p -> map.containsKey(p) || pred2link.containsKey(p)).collect(toList());
        }

        private @Nonnull SetMultimap<String, Link> getAtom2Link(@Nonnull Term predicate) {
            return map.computeIfAbsent(predicate, k -> MultimapBuilder.hashKeys(atomCount)
                                                                      .hashSetValues().build());
        }

        @Nonnull List<Atom> getExclusive() {
            return exclusive;
        }

        @Nonnull Stream<Link> streamNE(@Nonnull Term predicate, boolean reason) {
            Preconditions.checkArgument(predicate.isGround());
            if (!reason)
                return pred2link.get(predicate).stream();
            try {
                return predicatesInIndex.get(predicate).stream()
                        .flatMap(p -> pred2link.get(p).stream());
            } catch (ExecutionException e) { // should never throw
                throw new RuntimeException(e);
            }
        }

        @Nonnull Stream<Link> stream(@Nonnull Term predicate, @Nonnull Atom atom,
                                     @Nullable Triple.Position atomPosition,
                                     boolean reason) {
            if (atomPosition == Triple.Position.PRED)
                return Stream.empty();
            String name = atom.getName();
            if (predicate.isVar()) {
                if      (atomPosition == Triple.Position.SUBJ) return subj.get(name).stream();
                else if (atomPosition == Triple.Position.OBJ)  return  obj.get(name).stream();
                return Stream.concat(subj.get(name).stream(), obj.get(name).stream());
            }
            Stream<Link> stream;
            if (!reason) {
                SetMultimap<String, Link> mMap = map.get(predicate);
                stream = mMap == null ? Stream.empty() : mMap.get(name).stream();
            } else {
                try {
                    stream = predicatesInIndex.get(predicate).stream()
                                              .flatMap(p -> map.get(p).get(name).stream());
                } catch (ExecutionException e) {
                    throw new RuntimeException(e); // should never throw
                }
            }
            if (atomPosition == Triple.Position.SUBJ)
                return stream.filter(l -> l.s.getName().equals(name));
            else if (atomPosition == Triple.Position.OBJ)
                return stream.filter(l -> l.o.getName().equals(name));
            return stream.filter(l -> l.s.getName().equals(name) || l.o.getName().equals(name));
        }

        public boolean hasPredicate(@Nonnull Term predicate, boolean reason) {
            if (predicate.isVar())
                return true;
            if (reason) {
                try {
                    return !predicatesInIndex.get(predicate).isEmpty();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e); // never throws
                }
            } else {
                return map.containsKey(predicate) || pred2link.containsKey(predicate);
            }
        }
    }

    private final class State {
        private @Nonnull final CQuery parentQuery;
        private boolean reason;
        private @Nonnull Map<Term, CQuery> subQueries;
        private @Nonnull Map<ImmutablePair<Term, Atom>, List<List<LinkMatch>>> visited;
        private @Nonnull Multimap<ImmutablePair<Term, Atom>, LinkMatch>  incoming;
        private @Nonnull SemanticCQueryMatch.Builder builder;
        private @Nonnull Index idx;

        public State(@Nonnull CQuery query, boolean reason) {
            this.parentQuery = query;
            this.reason = reason;
            this.idx = getIndex();
            this.builder = SemanticCQueryMatch.builder(query);
            int count = query.size();
            HashSet<Term> sos = Sets.newHashSetWithExpectedSize(count * 2);
            for (Triple t : query) {
                sos.add(t.getSubject());
                sos.add(t.getObject());
            }
            subQueries = Maps.newHashMapWithExpectedSize(sos.size());
            List<Triple.Position> positions = asList(Triple.Position.SUBJ, Triple.Position.OBJ);
            for (Term term : sos)
                subQueries.put(term, query.containing(term, positions));
            int atomCount = molecule.getAtomCount();
            visited = new HashMap<>(count*atomCount);
            incoming = MultimapBuilder.hashKeys(count*atomCount)
                                      .arrayListValues().build();
        }

        public @Nonnull SemanticCQueryMatch build() {
            return builder.build();
        }

        public @Nonnull State matchNonExclusive() {
            for (Triple t : parentQuery) {
                if (t.getPredicate().isVar()) {
                    builder.addTriple(t).addAlternative(t, t);
                } else {
                    Iterator<Link> it = idx.streamNE(t.getPredicate(), reason).iterator();
                    if (it.hasNext())
                        builder.addTriple(t);
                    while (it.hasNext())
                        builder.addAlternative(t, t.withPredicate(it.next().p));
                }
            }
            return this;
        }

        public @Nonnull State matchExclusive() {
            Index idx = getIndex();
            // try all term - atom combinations.
            for (Map.Entry<Term, CQuery> entry : subQueries.entrySet()) {
                for (Atom atom : idx.getExclusive()) {
                    ImmutablePair<Term, Atom> termAtom = ImmutablePair.of(entry.getKey(), atom);
                    List<List<LinkMatch>> linkLists = findLinks(entry.getValue(), termAtom);
                    if (linkLists == null)
                        continue; // unsatisfiable
                    visited.put(termAtom, linkLists);
                    for (List<LinkMatch> list : linkLists) {
                        for (LinkMatch match : list)  incoming.put(match.getTo(), match);
                    }
                }
            }
            cascadeEliminations();
            // save remaining exclusive groups
            for (Map.Entry<ImmutablePair<Term, Atom>, List<List<LinkMatch>>> e : visited.entrySet()) {
                if (e.getValue() == null) continue;
                saveExclusiveGroup(subQueries.get(e.getKey().left), e.getValue());
            }
            return this;
        }

        private void cascadeEliminations() {
            Queue<ImmutablePair<Term, Atom>> queue = new ArrayDeque<>();
            for (Map.Entry<ImmutablePair<Term, Atom>, List<List<LinkMatch>>> e : visited.entrySet()) {
                if (e.getValue() != null) continue;
                for (LinkMatch linkMatch : incoming.get(e.getKey())) queue.add(linkMatch.from);
            }
            while (!queue.isEmpty()) {
                ImmutablePair<Term, Atom> pair = queue.remove();
                if (visited.remove(pair) == null)
                    continue; // was previously eliminated
                for (LinkMatch match : incoming.get(pair)) queue.add(match.from);
            }
        }

        @Nullable List<List<LinkMatch>> findLinks(@Nonnull CQuery query,
                                                  @Nonnull ImmutablePair<Term, Atom> termAtom) {
            Atom atom = termAtom.right;
            int linkCount = atom.getIn().size() + atom.getOut().size();
            if (atom.isClosed() && query.size() > linkCount)
                return null;
            ArrayList<List<LinkMatch>> linkLists = new ArrayList<>(query.size());
            int tripleIdx = -1;
            for (Triple triple : query) {
                int fTripleIdx = ++tripleIdx;
                Triple.Position pos = triple.where(termAtom.left);
                assert pos != null;
                ArrayList<LinkMatch> found = new ArrayList<>();
                idx.stream(triple.getPredicate(), atom, pos, reason)
                        .forEach(l -> found.add(new LinkMatch(l, termAtom, fTripleIdx, triple)));
                if (atom.isClosed() && found.isEmpty())
                    return null;
                linkLists.add(found);
            }
            return linkLists;
        }

        @SuppressWarnings("UnstableApiUsage")
        void saveExclusiveGroup(@Nonnull CQuery query, @Nonnull List<List<LinkMatch>> linkLists) {
            ArrayList<List<Term>> predicatesList = new ArrayList<>();
            ArrayList<Triple> subQuery = new ArrayList<>(query.size());
            HashSet<Term> temp = new HashSet<>();
            Iterator<Triple> queryIt = query.iterator();
            for (List<LinkMatch> list : linkLists) {
                Triple triple = queryIt.next();
                if (list.isEmpty())
                    continue;
                temp.clear();
                for (LinkMatch linkMatch : list) temp.add(linkMatch.l.p);
                predicatesList.add(new ArrayList<>(temp));
                subQuery.add(triple);
            }
            if (subQuery.isEmpty())
                return; //nothing to do
            if (subQuery.size() != query.size()) //avoid new instance creation
                query = CQuery.from(subQuery);
            builder.addExclusiveGroup(query);
            for (List<Term> ps : Lists.cartesianProduct(predicatesList)) {
                ImmutableList.Builder<Triple> b =
                        ImmutableList.builderWithExpectedSize(ps.size());
                assert ps.size() == query.size();
                Iterator<Term> it = ps.iterator();
                for (Triple triple : query) b.add(triple.withPredicate(it.next()));
                builder.addAlternative(query, CQuery.from(b.build()));
            }
        }

    }
}
