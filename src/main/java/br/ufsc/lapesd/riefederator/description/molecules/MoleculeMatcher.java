package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import java.lang.ref.SoftReference;
import java.util.*;

import static java.util.Arrays.asList;

@Immutable
public class MoleculeMatcher {
    private static final class LinkInfo {
        final @Nonnull Atom reference;
        final boolean incoming;
        final @Nonnull MoleculeLink link;

        public LinkInfo(@Nonnull Atom reference, @Nonnull MoleculeLink link, boolean incoming) {
            this.reference = reference;
            this.link = link;
            this.incoming = incoming;
        }
        public boolean isIncoming() {
            return incoming;
        }
        public @Nonnull Atom getSubject() {
            return incoming ? link.getAtom() : reference;
        }
        public @Nonnull Atom getObject() {
            return incoming ? reference : link.getAtom();
        }
        public @Nonnull Atom getOpposite(@Nonnull Atom other) {
            return reference.getName().equals(other.getName()) ? link.getAtom() : reference;
        }
        public @Nonnull Term getOpposite(@Nonnull Atom other,
                                         @Nonnull Triple triple) {
            if (reference.getName().equals(other.getName())) {
                return incoming ? triple.getSubject() : triple.getObject();
            } else {
                return incoming ? triple.getObject() : triple.getSubject();
            }
        }
        public @Nonnull Term getReferenceTerm(@Nonnull Triple triple) {
            return incoming ? triple.getObject() : triple.getSubject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LinkInfo)) return false;
            LinkInfo linkInfo = (LinkInfo) o;
            return incoming == linkInfo.incoming &&
                    reference.equals(linkInfo.reference) &&
                    link.equals(linkInfo.link);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reference, incoming, link);
        }
    }

    @SuppressWarnings("Immutable") @Nonnull @LazyInit
    private SoftReference<Multimap<Term, LinkInfo>> linkIndex = new SoftReference<>(null);
    @SuppressWarnings("Immutable") @Nonnull @LazyInit
    private SoftReference<Map<String, Multimap<Term, LinkInfo>>> linkAtomIndex
            = new SoftReference<>(null);
    private final @Nonnull Molecule molecule;

    public MoleculeMatcher(@Nonnull Molecule molecule) {
        this.molecule = molecule;
    }

    private class LinkGetter {
        private Multimap<Term, LinkInfo> map = linkIndex.get();

        public LinkGetter() {
            synchronized (MoleculeMatcher.this) {
                if (map == null) {
                    map = HashMultimap.create(32, 2);
                    Set<Atom> visited = new HashSet<>();
                    ArrayDeque<Atom> stack = new ArrayDeque<>();
                    stack.push(molecule.getCore());
                    while (!stack.isEmpty()) {
                        Atom atom = stack.pop();
                        if (!visited.add(atom)) continue;
                        atom.getIn ().forEach(l -> stack.push(l.getAtom()));
                        atom.getOut().forEach(l -> stack.push(l.getAtom()));
                        for (MoleculeLink link : atom.getIn())
                            map.put(link.getEdge(), new LinkInfo(atom, link, true));
                        for (MoleculeLink link : atom.getOut())
                            map.put(link.getEdge(), new LinkInfo(atom, link, false));
                    }
                    linkIndex = new SoftReference<>(map);
                }
            }
        }

        public @Nonnull Collection<LinkInfo> get(@Nonnull Term predicate) {
            return predicate.isGround() ? map.get(predicate) : map.values();
        }
    }

    private class AtomLinkGetter {
        private final @Nonnull Map<String, Multimap<Term, LinkInfo>> map;

        public AtomLinkGetter() {
            synchronized (MoleculeMatcher.this) {
                LinkGetter linkGetter = new LinkGetter();
                Map<String, Multimap<Term, LinkInfo>> local = linkAtomIndex.get();
                if (local == null) {
                    local = Maps.newHashMapWithExpectedSize(molecule.getAtomCount());
                    for (LinkInfo i : linkGetter.map.values()) {
                        Multimap<Term, LinkInfo> mm = local.computeIfAbsent(i.reference.getName(),
                                k -> HashMultimap.create(i.reference.edgesCount(), 4));
                        mm.put(i.link.getEdge(), i);
                        // record also for the other counterpart
                        mm = local.computeIfAbsent(i.link.getAtom().getName(),
                                k -> HashMultimap.create(i.reference.edgesCount(), 4));
                        mm.put(i.link.getEdge(), i);
                    }
                    linkAtomIndex = new SoftReference<>(local);
                }
                map = local;
            }
        }

        public @Nonnull Collection<LinkInfo> get(@Nonnull Atom atom, @Nonnull Term predicate) {
            Multimap<Term, LinkInfo> mm = map.get(atom.getName());
            if (predicate.isGround())
                return mm == null ? Collections.emptyList() : mm.get(predicate);
            return mm.values();
        }
    }

    private final  class State {
        private final @Nonnull  CQuery parentQuery;
        private final @Nonnull  List<List<Triple>> exclusiveGroups;
        private final @Nonnull  Set<Triple> nonExclusiveRelevant;
        private final @Nonnull HashMap<ImmutablePair<Term, Atom>, Boolean> visited;
        private final @Nonnull Map<Term, Atom> lastMatched;
        private final @Nonnull Map<Term, CQuery> subQueries;
        private boolean built = false;

        public State(@Nonnull CQuery query) {
            this.parentQuery = query;
            int count = query.size();
            exclusiveGroups = new ArrayList<>(count);
            nonExclusiveRelevant = Sets.newHashSetWithExpectedSize(count);
            visited = Maps.newHashMapWithExpectedSize(molecule.getAtomCount()*2);
            lastMatched = Maps.newHashMapWithExpectedSize(count*2);

            HashSet<Term> sos = Sets.newHashSetWithExpectedSize(count * 2);
            for (Triple t : query) {
                sos.add(t.getSubject());
                sos.add(t.getObject());
            }
            subQueries = Maps.newHashMapWithExpectedSize(sos.size());
            List<Triple.Position> positions = asList(Triple.Position.SUBJ, Triple.Position.OBJ);
            for (Term term : sos)
                subQueries.put(term, query.containing(term, positions));
        }

        @Contract(value = "-> new")
        @WillClose
        public @Nonnull CQueryMatch build() {
            Preconditions.checkState(!built, "State already built into a CQueryMatch");
            built = true;
            CQueryMatch.Builder builder = CQueryMatch.builder(parentQuery);
            exclusiveGroups.forEach(builder::addExclusiveGroup);
            nonExclusiveRelevant.forEach(builder::addTriple);
            return builder.build();
        }

        private class AtomBinding {
            private final @Nullable Term bound;
            private final boolean valid;

            private AtomBinding(@Nullable Term bound, boolean valid) {
                this.bound = bound;
                this.valid = valid;
            }
            public boolean isValid() {
                return valid;
            }
            @Contract("_ -> param1") @CanIgnoreReturnValue
            public boolean rollbackIf(boolean value) {
                if (value)
                    lastMatched.remove(bound);
                return value;
            }

            @Override
            public @Nonnull String toString() {
                return String.format("AtomBinding(%s, %s)", valid, bound);
            }
        }

        /**
         * Records that a term was bound to an {@link Atom} in this molecule. The binding may
         * be rejected by disjunction axioms within the molecule given previous bindings.
         *
         * @return true iff the binding was accepted.
         */
        private @Nonnull AtomBinding bindAtom(@Nonnull Term term, @Nonnull Atom atom) {
            Atom old = lastMatched.get(term);
            if (old != null && (old.isDisjoint() || atom.isDisjoint())) {
                boolean ok = old.getName().equals(atom.getName());
                assert !ok || old.equals(atom) : "Non-unique atom names: " + old + ", " + atom;
                return new AtomBinding(null, ok);
            }
            lastMatched.put(term, atom);
            return new AtomBinding(term, true);
        }

        @Contract("-> this")
        public @Nonnull State start() {
            LinkGetter lGetter = new LinkGetter();
            // explore the possible exclusive groups
            for (Map.Entry<Term, CQuery> entry : subQueries.entrySet()) {
                Term term = entry.getKey();
                for (Triple triple : entry.getValue()) {
                    for (LinkInfo info : lGetter.get(triple.getPredicate())) {
                        Atom atom = info.reference;
                        if (atom.isExclusive() && info.getReferenceTerm(triple).equals(term))
                            visit(term, atom, entry.getValue());
                    }
                }
            }

            // only try these atom-bindings after we exhausted alternatives for
            // the exclusive group candidates.
            Set<Term> contradicted = new HashSet<>(subQueries.size());
            for (Triple triple : parentQuery) {
                for (LinkInfo info : lGetter.get(triple.getPredicate())) {
                    if (info.reference.isExclusive()) continue;
                    boolean fail = false;
                    if (!bindAtom(triple.getSubject(), info.getSubject()).isValid())
                        fail  = contradicted.add(triple.getSubject());
                    if (!bindAtom(triple.getObject(), info.getObject()).isValid())
                        fail |= contradicted.add(triple.getObject());
                    if (!fail)
                        nonExclusiveRelevant.add(triple);
                }
            }
            nonExclusiveRelevant.removeIf(t ->
                    contradicted.contains(t.getSubject()) || contradicted.contains(t.getObject()));
            return this;
        }

        public boolean visit(@Nonnull Term term, @Nonnull Atom atom, @Nonnull CQuery atomQuery) {
            if (atomQuery.isEmpty())
                return true; //consider a a match bcs we have no evidence to cascade a failure
            ImmutablePair<Term, Atom> pair = ImmutablePair.of(term, atom);
            Boolean cached = visited.get(pair);
            if (cached != null)
                return cached; // visit already complete or in progress
            AtomBinding binding = bindAtom(term, atom);//check disjointness
            visited.put(pair, binding.isValid()); //assume true until we fail
            boolean ok = binding.isValid()
                    && (!atom.isExclusive() || visitExclusive(atom, atomQuery));
            if (!ok)
                visited.put(pair, false); // record failure
            return !binding.rollbackIf(!ok);
        }

        private boolean visitExclusive(@Nonnull Atom atom, @Nonnull CQuery qry) {
            AtomLinkGetter alGetter = new AtomLinkGetter();
            List<Triple> satisfied = new ArrayList<>(qry.size());

            for (Triple t : qry) {
                boolean found = false;
                for (LinkInfo info : alGetter.get(atom, t.getPredicate())) {
                    Term oppTerm = info.getOpposite(atom, t);
                    Atom oppAtom = info.getOpposite(atom);
                    if ((found = visit(oppTerm, oppAtom, subQueries.get(oppTerm)))) {
                        satisfied.add(t);
                        break; // no need for further exploration
                    }
                }
                if (!found && atom.isClosed())
                    return false; // zero'd out the query results. stop trying
            }
            exclusiveGroups.add(satisfied); // save the group for later
            return true;
        }
    }

    public @Nonnull CQueryMatch match(@Nonnull CQuery query) {
        return new State(query).start().build();
    }
}
