package br.ufsc.lapesd.freqel.description.molecules;

import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
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
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

@Immutable
public class MoleculeMatcherWithDisjointness implements Description {
    private static final class LinkInfo {
        final @Nonnull Atom reference;
        final boolean incoming;
        final @Nonnull MoleculeLink link;

        public LinkInfo(@Nonnull Atom reference, @Nonnull MoleculeLink link, boolean incoming) {
            this.reference = reference;
            this.link = link;
            this.incoming = incoming;
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
    @SuppressWarnings("Immutable") // it really is immutable but errorprone can't prove it.
    private final @Nonnull Set<Triple> BAD_SET = singleton(null);

    public MoleculeMatcherWithDisjointness(@Nonnull Molecule molecule) {
        this.molecule = molecule;
    }

    private class LinkGetter {
        private Multimap<Term, LinkInfo> map = linkIndex.get();

        public LinkGetter() {
            synchronized (MoleculeMatcherWithDisjointness.this) {
                if (map == null) {
                    map = HashMultimap.create(32, 2);
                    Set<String> visited = new HashSet<>();
                    ArrayDeque<Atom> stack = new ArrayDeque<>();
                    molecule.getCores().forEach(stack::push);
                    while (!stack.isEmpty()) {
                        Atom atom = stack.pop();
                        if (!visited.add(atom.getName())) continue;
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
            synchronized (MoleculeMatcherWithDisjointness.this) {
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
        private final @Nonnull  Set<Set<Triple>> exclusiveGroups;
        private final @Nonnull  Set<Triple> nonExclusiveRelevant;
        private final @Nonnull HashMap<ImmutablePair<Term, Atom>, Set<Triple>> visited;
        private final @Nonnull Map<Term, Atom> lastMatched;
        private final @Nonnull Map<Term, CQuery> subQueries;
        private boolean built = false;

        public State(@Nonnull CQuery query) {
            this.parentQuery = query;
            int count = query.size();
            exclusiveGroups = Sets.newHashSetWithExpectedSize(count);
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
            private final @Nonnull List<Term> bound;
            private final boolean valid;

            private AtomBinding() {
                this(null, true);
            }
            private AtomBinding(@Nullable Term bound, boolean valid) {
                this.bound = new ArrayList<>(parentQuery.size());
                if (bound != null)
                    this.bound.add(bound);
                this.valid = valid;
            }
            public void addAll(@Nonnull AtomBinding other) {
                this.bound.addAll(other.bound);
            }

            public boolean isValid() {
                return valid;
            }

            public void rollback() {
                bound.forEach(lastMatched::remove);
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
                if (!ok) {
                    visited.put(ImmutablePair.of(term, atom), BAD_SET); // term is contradictory
                    visited.put(ImmutablePair.of(term, old ), BAD_SET); // term is contradictory
                }
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
                            visit(term, atom, entry.getValue(), null);
                    }
                }
            }

            // only try these atom-bindings after we exhausted alternatives for
            // the exclusive group candidates.
            for (Triple triple : parentQuery) {
                for (LinkInfo info : lGetter.get(triple.getPredicate())) {
                    if (info.reference.isExclusive()) continue;
                    nonExclusiveRelevant.add(triple);
                }
            }
            return this;
        }

        public Set<Triple> visit(@Nonnull Term term, @Nonnull Atom atom,
                                 @Nonnull CQuery atomQuery,
                                 @Nullable AtomBinding parentBinding) {
            if (atomQuery.isEmpty())
                return emptySet(); //consider a a match due to lack of evidence

            ImmutablePair<Term, Atom> pair = ImmutablePair.of(term, atom);
            Set<Triple> matched = visited.get(pair);
            if (matched != null)
                return matched == BAD_SET ? null : matched; // visit already complete or in progress


            AtomBinding binding = bindAtom(term, atom); //check disjointness
            visited.put(pair, binding.isValid() ? emptySet() : BAD_SET); //assume ok until we fail

            if (binding.isValid()) //only recurse if valid and exclusive
                matched = atom.isExclusive() ? visitExclusive(atom,atomQuery,binding) : emptySet();

            if (matched == null) { // failed. Undo atom bindings & record failure
                visited.put(pair, BAD_SET);
                binding.rollback();
            } else if (parentBinding != null) { // not a failure
                visited.put(pair, matched);
                parentBinding.addAll(binding);
            }

            return matched;
        }

        @Nullable
        private Set<Triple> visitExclusive(@Nonnull Atom atom, @Nonnull CQuery qry,
                                           @Nonnull AtomBinding parentBinding) {
            AtomLinkGetter alGetter = new AtomLinkGetter();
            Set<Triple> satisfied = Sets.newHashSetWithExpectedSize(parentQuery.size());
            List<Set<Triple>> absorbed = new ArrayList<>(parentQuery.size());
            AtomBinding local = new AtomBinding();

            for (Triple t : qry) {
                Set<Triple> found = null;
                for (LinkInfo info : alGetter.get(atom, t.getPredicate())) {
                    Term oppTerm = info.getOpposite(atom, t);
                    Atom oppAtom = info.getOpposite(atom);
                    if ((found = visit(oppTerm, oppAtom, subQueries.get(oppTerm), local)) != null){
                        satisfied.add(t);
                        absorbed.add(found);
                        satisfied.addAll(found);
                        break; // no need for further exploration
                    }
                }
                if (found == null && atom.isClosed()) {
                    local.rollback();
                    return null; // zero'd out the query results. stop trying
                }
            }
            if (satisfied.isEmpty()) {
                local.rollback();
                return null;
            } else {
                absorbed.forEach(exclusiveGroups::remove);
                if (exclusiveGroups.stream().noneMatch(g -> g.containsAll(satisfied)))
                    exclusiveGroups.add(satisfied); // save the group for later
                parentBinding.addAll(local);
                return satisfied;
            }
        }
    }

    @Override
    public @Nonnull CQueryMatch match(@Nonnull CQuery query, @Nonnull MatchReasoning reasoning) {
        return new State(query).start().build();
    }

    @Override public @Nonnull CQueryMatch localMatch(@Nonnull CQuery query,
                                                      @Nonnull MatchReasoning reasoning) {
        return match(query, reasoning);
    }

    @Override public boolean supports(@Nonnull MatchReasoning mode) {
        return MatchReasoning.NONE.equals(mode) || MatchReasoning.ALTERNATIVES.equals(mode);
    }

    @Override
    public void update() {
        /* no op */
    }

    @Override
    public void init() {
        /* no op */
    }

    @Override
    public boolean waitForInit(int timeoutMilliseconds) {
        return true; /* alway initialized */
    }

    @Override
    public boolean updateSync(int timeoutMilliseconds) {
        return true; //no op
    }

    @Override
    public @Nonnull String toString() {
        return String.format("MoleculeMatcherWithDisjointness(%s)", molecule);
    }
}
