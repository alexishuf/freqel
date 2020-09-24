package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.description.MatchAnnotation;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.util.indexed.*;
import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.SimpleImmIndexSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class CQueryCache {
    private @Nonnull final CQueryData d;
    private List<Triple> unmodifiableList;
    private Set<QueryAnnotation> unmodifiableQueryAnnotations;

    private ImmIndexSet<Triple> set;
    private ImmIndexSet<Term> allTerms;
    private IndexSetPartition<Term> tripleTerms;
    private ImmIndexSubset<Var> allVars;
    private ImmIndexSubset<Var> tripleVars;
    private IndexSet<Triple> offeredTriplesUniverse;
    private IndexSet<String> varNamesUniverse, offeredVarNamesUniverse;
    private ImmIndexSet<String> allVarNames;
    private ImmIndexSet<String> tripleVarNames;
    private ImmIndexSubset<String> publicTripleVarNames;
    private ImmIndexSet<String> publicVarNames;
    private ImmIndexSubset<String> inputVarNames, reqInputVarNames, optInputVarNames;

    private final AtomicInteger indexTriplesState = new AtomicInteger(0);
    private SoftReference<List<ImmIndexSubset<Triple>>> s2triple, p2triple, o2triple;
    private SoftReference<SetMultimap<Term, Atom>> t2atom;
    private ImmIndexSet<Triple> matchedTriples;

    private Boolean joinConnected, ask;
    private int limit = -1;
    private int queryHash = 0;

    public CQueryCache(@Nonnull CQueryData data) {
        this.d = data;
    }

    public CQueryCache(@Nonnull CQueryData data, @Nonnull CQueryCache other) {
        this.d = data;
        set = other.set;
        unmodifiableList = other.unmodifiableList;
        unmodifiableQueryAnnotations = other.unmodifiableQueryAnnotations;
        tripleTerms = other.tripleTerms;
        allTerms = other.allTerms;
        tripleVars = other.tripleVars;
        allVars = other.allVars;
        offeredVarNamesUniverse = other.offeredVarNamesUniverse;
        offeredTriplesUniverse = other.offeredTriplesUniverse;
        tripleVarNames = other.tripleVarNames;
        allVarNames = other.allVarNames;
        varNamesUniverse = other.varNamesUniverse;
        inputVarNames = other.inputVarNames;
        reqInputVarNames = other.reqInputVarNames;
        optInputVarNames = other.optInputVarNames;
        publicTripleVarNames = other.publicTripleVarNames;
        publicVarNames = other.publicVarNames;
        indexTriplesState.set(other.indexTriplesState.get());
        s2triple = other.s2triple;
        p2triple = other.p2triple;
        o2triple = other.o2triple;
        t2atom = other.t2atom;
        matchedTriples = other.matchedTriples;
        joinConnected = other.joinConnected;
        ask = other.ask;
        limit = other.limit;
        queryHash = other.queryHash;
    }

    void invalidateAllTerms() {
        allTerms = null;
    }

    void invalidateTriples() {
        set = null;
        unmodifiableList = null;
        tripleTerms = null;
        allTerms = null;
        tripleVars = allVars = null;
        tripleVarNames = null;
        allVarNames = null;
        varNamesUniverse = null;
        inputVarNames = null;
        reqInputVarNames = null;
        optInputVarNames = null;
        publicTripleVarNames = null;
        publicVarNames = null;
        indexTriplesState.set(0);
        s2triple = p2triple = o2triple = null;
        t2atom = null;
        matchedTriples = null;
        joinConnected = null;
        ask = null;
        queryHash = 0;
    }

    void invalidateQueryAnnotations() {
        unmodifiableQueryAnnotations = null;
        queryHash = 0;
    }

    void notifyTermAnnotationChange(Class<? extends TermAnnotation> annClass) {
        if (AtomAnnotation.class.isAssignableFrom(annClass)) {
            t2atom = null;
            inputVarNames = null;
            reqInputVarNames = null;
            optInputVarNames = null;
        }
        queryHash = 0;
    }

    void invalidateTermAnnotations() {
        t2atom = null;
        inputVarNames = null;
        reqInputVarNames = null;
        optInputVarNames = null;
        queryHash = 0;
    }

    void notifyTripleAnnotationChange(Class<? extends TripleAnnotation> annClass) {
        if (MatchAnnotation.class.isAssignableFrom(annClass))
            matchedTriples = null;
        queryHash = 0;
    }

    void invalidateTripleAnnotations() {
        matchedTriples = null;
        queryHash = 0;
    }

    void notifyModifierChange(Class<? extends Modifier> modClass) {
        if (SPARQLFilter.class.isAssignableFrom(modClass)) {
            allVars = null;
            allVarNames = null;
            varNamesUniverse = null;
            allTerms = null;
            publicVarNames = null;
            inputVarNames = null;
            reqInputVarNames = null;
            optInputVarNames = null;
        } else if (Limit.class.isAssignableFrom(modClass)) {
            limit = -1;
        } else if (Ask.class.isAssignableFrom(modClass)) {
            ask = null;
            publicVarNames = null;
            publicTripleVarNames = null;
        } else if (Projection.class.isAssignableFrom(modClass)) {
            publicVarNames = null;
            publicTripleVarNames = null;
        }
        queryHash = 0;
    }

    public boolean offerVarNamesUniverse(@Nonnull IndexSet<String> universe) {
        if (offeredVarNamesUniverse != universe) {
            offeredVarNamesUniverse = universe;
            tripleVarNames = allVarNames = publicVarNames = null;
            reqInputVarNames = optInputVarNames = inputVarNames = null;
            varNamesUniverse = null;
            publicTripleVarNames = null;
            return true;
        }
        return false;
    }

    public @Nullable IndexSet<String> varNamesUniverseOffer() {
        return offeredVarNamesUniverse;
    }

    public boolean offerTriplesUniverse(@Nonnull IndexSet<Triple> universe) {
        if (offeredTriplesUniverse != universe) {
            offeredTriplesUniverse = universe;
            set = matchedTriples = null;
            indexTriplesState.set(0);
            s2triple = p2triple = o2triple = null;
            return true;
        }
        return false;
    }

    public @Nullable IndexSet<Triple> triplesUniverseOffer() {
        return offeredTriplesUniverse;
    }

    public @Nonnull IndexSet<Triple> getSet() {
        if (set == null) {
            if (offeredTriplesUniverse != null)
                set = offeredTriplesUniverse.immutableSubsetExpanding(d.list);
            else
                set = FullIndexSet.from(d.list).asImmutable();
        }
        return set;
    }

    public @Nonnull List<Triple> unmodifiableList() {
        if (unmodifiableList == null)
            unmodifiableList = Collections.unmodifiableList(d.list);
        return unmodifiableList;
    }

    public @Nonnull Set<QueryAnnotation> unmodifiableQueryAnnotations() {
        if (unmodifiableQueryAnnotations == null)
            unmodifiableQueryAnnotations = unmodifiableSet(d.queryAnns);
        return unmodifiableQueryAnnotations;
    }

    public @Nonnull IndexSet<Term> tripleTerms() {
        if (tripleTerms == null)
            indexTerms();
        return tripleTerms;
    }

    public @Nonnull ImmIndexSubset<Var> tripleVars() {
        if (tripleVars == null) {
            IndexSet<Term> terms = tripleTerms();
            BitSet bs = new BitSet(terms.size());
            int i = 0;
            for (Term term : terms) {
                if (term.isVar())
                    bs.set(i);
                ++i;
            }
            @SuppressWarnings("UnnecessaryLocalVariable") IndexSet<?> erased = terms;
            //noinspection unchecked
            tripleVars = new SimpleImmIndexSubset<>((IndexSet<Var>) erased, bs);
        }
        return tripleVars;
    }

    private void indexTerms() {
        int capacity = d.list.size()*3 + d.modifiers.filters().size()*2;
        FullIndexSet<Term> allTerms = new FullIndexSet<>(capacity);
        for (Triple triple : d.list) {
            allTerms.add(triple.getSubject());
            allTerms.add(triple.getPredicate());
            allTerms.add(triple.getObject());
        }
        int tripleTermsCount = allTerms.size();
        for (SPARQLFilter filter : d.modifiers.filters()) {
            for (Term term : filter.getTerms())
                allTerms.add(term);
        }
        this.allTerms = allTerms.asImmutable();
        tripleTerms = IndexSetPartition.of(this.allTerms, 0, tripleTermsCount);
    }

    public @Nonnull IndexSet<Term> allTerms() {
        if (allTerms == null)
            indexTerms();
        return allTerms;
    }

    public @Nonnull ImmIndexSet<Var> allVars() {
        if (allVars == null) {
            IndexSet<Term> terms = allTerms();
            BitSet bs = (BitSet) tripleVars().getBitSet().clone();
            for (int i = tripleTerms.getEnd(); i < terms.size(); i++) {
                Term term = terms.get(i);
                if (term.isVar())
                    bs.set(i);
            }
            @SuppressWarnings("UnnecessaryLocalVariable") IndexSet<?> cleared = terms;
            //noinspection unchecked
            allVars = new SimpleImmIndexSubset<>((IndexSet<Var>) cleared, bs);
        }
        return allVars;
    }

    public @Nonnull ImmIndexSet<String> tripleVarNames() {
        if (tripleVarNames == null)
            indexVarNames();
        return tripleVarNames;
    }

    public @Nonnull ImmIndexSet<String> allVarNames() {
        if (allVarNames == null)
            indexVarNames();
        return allVarNames;
    }

    private @Nonnull IndexSet<String> varNamesUniverse() {
        if (varNamesUniverse == null)
            indexVarNames();
        return varNamesUniverse;
    }

    private void indexVarNames() {
        if (offeredVarNamesUniverse != null) indexVarNamesFromOffered();
        else                                 indexVarNamesFromScratch();
    }

    private void indexVarNamesFromOffered() {
        IndexSubset<String> allVarNames = offeredVarNamesUniverse.emptySubset();
        for (Triple t : d.list) {
            Term s = t.getSubject(), p = t.getPredicate(), o = t.getObject();
            if (s.isVar()) allVarNames.parentAdd(s.asVar().getName());
            if (p.isVar()) allVarNames.parentAdd(p.asVar().getName());
            if (o.isVar()) allVarNames.parentAdd(o.asVar().getName());
        }
        tripleVarNames = allVarNames.immutableCopy();
        for (SPARQLFilter filter : d.modifiers.filters()) {
            for (String name : filter.getVarNames())
                allVarNames.parentAdd(name);
        }
        this.allVarNames = allVarNames.asImmutable();
        this.varNamesUniverse = CoWIndexSet.shared(offeredVarNamesUniverse);
    }

    private void indexVarNamesFromScratch() {
        IndexSet<Term> terms = allTerms();
        FullIndexSet<String> univ = new FullIndexSet<>(d.list.size() * 2);
        int tripleVarsEnd = -1, termsIdx  = -1, tripleTermsEnd = tripleTerms.getEnd();
        for (Term term : terms) {
            ++termsIdx;
            if (term.isVar()) {
                if (termsIdx >= tripleTermsEnd && tripleVarsEnd < 0)
                    tripleVarsEnd = univ.size();
                univ.add(term.asVar().getName());
            }
        }
        if (tripleVarsEnd < 0)
            tripleVarsEnd = univ.size();
        tripleVarNames = IndexSetPartition.of(univ, 0, tripleVarsEnd);
        allVarNames = IndexSetPartition.of(univ, 0, univ.size());
        Projection projection = d.modifiers.projection();
        if (projection != null)
            univ.addAll(projection.getVarNames());
        varNamesUniverse = univ;
    }

    public @Nonnull ImmIndexSet<String> publicVarNames() {
        if (publicVarNames == null) {
            if (d.modifiers.ask() != null) {
                IndexSet<String> set;
                if (varNamesUniverse != null)             set = varNamesUniverse;
                else if (offeredVarNamesUniverse != null) set = offeredVarNamesUniverse;
                else                                      set = ImmFullIndexSet.empty();
                return publicVarNames = set.immutableEmptySubset();
            }
            Projection projection = d.modifiers.projection();
            if (projection == null)
                return publicVarNames = allVarNames();
            if (varNamesUniverse == null)
                return varNamesUniverse().immutableSubset(projection.getVarNames());

            IndexSubset<String> subset = varNamesUniverse.emptySubset();
            subset.parentAddAll(projection.getVarNames());
            publicVarNames = subset.asImmutable();
            assert varNamesUniverse.containsAll(projection.getVarNames());
            assert publicVarNames.containsAll(projection.getVarNames());
        }
        return publicVarNames;
    }

    public @Nonnull ImmIndexSubset<String> publicTripleVarNames() {
        if (publicTripleVarNames == null) {
            publicTripleVarNames = publicVarNames().fullSubset().intersect(tripleVarNames())
                                                   .asImmutable();
        }
        return publicTripleVarNames;
    }

    private void scanInputs() {
        IndexSet<String> allVarNames = allVarNames();
        IndexSubset<String> req = allVarNames.emptySubset(), opt = allVarNames.emptySubset();
        IndexSet<String> parent = req.getParent();
        IndexSet<String> tripleVarNames = tripleVarNames();
        for (Map.Entry<Term, Collection<TermAnnotation>> e : d.termAnns.asMap().entrySet()) {
            Term term = e.getKey();
            if (!term.isVar()) continue;
            String name = term.asVar().getName();
            for (TermAnnotation a : e.getValue()) {
                if (!(a instanceof AtomInputAnnotation))
                    continue;
                AtomInputAnnotation ia = (AtomInputAnnotation) a;
                if (ia.isOverride() && requireNonNull(ia.getOverrideValue()).isGround())
                    continue; //not a input anymore
                (ia.isRequired() ? req : opt).add(name);
            }
        }
        for (Iterator<Map.Entry<String, Integer>> it = allVarNames.entryIterator(); it.hasNext(); ){
            Map.Entry<String, Integer> e = it.next();
            int idx = e.getValue();
            if (!opt.hasIndex(idx, parent) && !tripleVarNames.contains(e.getKey()))
                req.setIndex(idx, parent); //FILTER input var
        }
        reqInputVarNames = req.asImmutable();
        optInputVarNames = opt.asImmutable();
    }

    public @Nonnull ImmIndexSubset<String> inputVarNames() {
        if (inputVarNames == null) {
            IndexSubset<String> set = allVarNames().emptySubset();
            set.addAll(reqInputVarNames());
            set.addAll(optInputVarNames());
            inputVarNames = set.immutableCopy();
        }
        return inputVarNames;
    }
    public @Nonnull ImmIndexSubset<String> reqInputVarNames() {
        if (reqInputVarNames == null)
            scanInputs();
        return reqInputVarNames;
    }
    public @Nonnull ImmIndexSubset<String> optInputVarNames() {
        if (optInputVarNames == null)
            scanInputs();
        return optInputVarNames;
    }

    public @Nonnull IndexSet<Triple> matchedTriples() {
        if (matchedTriples == null) {
            if (offeredTriplesUniverse != null) {
                try {
                    return matchedTriples = fillMatched(offeredTriplesUniverse.emptySubset());
                } catch (NotInParentException ignored) {  }
            }
            matchedTriples = fillMatched(new FullIndexSet<>(d.list.size()));
        }
        return matchedTriples;
    }

    private @Nonnull ImmIndexSet<Triple> fillMatched(@Nonnull IndexSet<Triple> set) {
        for (Triple triple : d.list) {
            boolean has = false;
            for (TripleAnnotation a : d.tripleAnns.get(triple)) {
                if (a instanceof MatchAnnotation) {
                    has = true;
                    Triple matched = ((MatchAnnotation) a).getMatched();
                    set.safeAdd(matched);
                }
            }
            if (!has)
                set.safeAdd(triple);
        }
        return set.asImmutable();
    }

    private void indexTriples() {
        if (!indexTriplesState.compareAndSet(0, 1)) {
            // already initialized or other thread is initializing, spinlock
            while (indexTriplesState.get() != 2)
                Thread.yield();
            return;
        }
        IndexSet<Triple> triples = getSet();
        IndexSet<Triple> triplesUniverse = triples.getParent();
        IndexSet<Term> terms = tripleTerms();
        int termCount = terms.size(), tripleCount = d.list.size();
        BitSet[] sSets = new BitSet[termCount], pSets = new BitSet[termCount],
                 oSets = new BitSet[termCount];
        if (triples instanceof IndexSubset) {
            for (Triple t : d.list) {
                int sIdx = terms.indexOf(t.getSubject()), oIdx = terms.indexOf(t.getObject());
                int pIdx = terms.indexOf(t.getPredicate()), i = triplesUniverse.indexOf(t);
                (sSets[sIdx] == null ? sSets[sIdx] = new BitSet(tripleCount) : sSets[sIdx]).set(i);
                (pSets[pIdx] == null ? pSets[pIdx] = new BitSet(tripleCount) : pSets[pIdx]).set(i);
                (oSets[oIdx] == null ? oSets[oIdx] = new BitSet(tripleCount) : oSets[oIdx]).set(i);
            }
        } else {
            assert new ArrayList<>(triples).equals(d.list);
            for (int i = 0; i < tripleCount; i++) {
                Triple t = d.list.get(i);
                int sIdx = terms.indexOf(t.getSubject()), oIdx = terms.indexOf(t.getObject());
                int pIdx = terms.indexOf(t.getPredicate());
                (sSets[sIdx] == null ? sSets[sIdx] = new BitSet(tripleCount) : sSets[sIdx]).set(i);
                (pSets[pIdx] == null ? pSets[pIdx] = new BitSet(tripleCount) : pSets[pIdx]).set(i);
                (oSets[oIdx] == null ? oSets[oIdx] = new BitSet(tripleCount) : oSets[oIdx]).set(i);
            }
        }
        s2triple = toImmIndexedSubset(triplesUniverse, sSets);
        p2triple = toImmIndexedSubset(triplesUniverse, pSets);
        o2triple = toImmIndexedSubset(triplesUniverse, oSets);
        indexTriplesState.set(2);
    }

    private static final @Nonnull BitSet EMPTY_BITSET = new BitSet();

    private static @Nonnull <T> SoftReference<List<ImmIndexSubset<T>>>
    toImmIndexedSubset(@Nonnull IndexSet<T> parent, @Nonnull BitSet[] sets) {
        List<ImmIndexSubset<T>> list = new ArrayList<>(sets.length);
        for (BitSet set : sets)
            list.add(parent.immutableSubset(set == null ? EMPTY_BITSET : set));
        return new SoftReference<>(list);
    }

    public @Nonnull ImmIndexSubset<Triple> triplesWithTerm(@Nonnull Term term) {
        IndexSet<Triple> triples = getSet();
        int idx = tripleTerms().indexOf(term);
        if (idx < 0)
            return triples.immutableEmptySubset();
        List<ImmIndexSubset<Triple>> s2t, p2t, o2t;
        while ((s2t = s2triple == null ? null : s2triple.get()) == null) indexTriples();
        while ((p2t = p2triple == null ? null : p2triple.get()) == null) indexTriples();
        while ((o2t = o2triple == null ? null : o2triple.get()) == null) indexTriples();
        BitSet set = new BitSet();
        set.or(s2t.get(idx).getBitSet());
        set.or(p2t.get(idx).getBitSet());
        set.or(o2t.get(idx).getBitSet());
        return triples.getParent().immutableSubset(set);
    }

    public @Nonnull IndexSubset<Triple> triplesWithTermAt(@Nonnull Term term,
                                                          @Nonnull Triple.Position position) {
        int termIdx = tripleTerms().indexOf(term);
        if (termIdx < 0)
            return getSet().immutableEmptySubset();
        List<ImmIndexSubset<Triple>> list = null;
        while (list == null) {
            indexTriples();
            switch (position) {
                case SUBJ:
                    list = s2triple == null ? null : s2triple.get(); break;
                case PRED:
                    list = p2triple == null ? null : p2triple.get(); break;
                case OBJ:
                    list = o2triple == null ? null : o2triple.get(); break;
                default:
                    throw new IllegalArgumentException("Bad position="+position);
            }
        }
        return list.get(termIdx);
    }

    public @Nonnull SetMultimap<Term, Atom> termAtoms() {
        SetMultimap<Term, Atom> strong = t2atom == null ? null : t2atom.get();
        if (strong != null)
            return strong;
        SetMultimap<Term, Atom> map = HashMultimap.create(tripleTerms().size(), 2);
        for (Map.Entry<Term, TermAnnotation> e : d.termAnns.entries()) {
            if (e.getValue() instanceof AtomAnnotation)
                map.put(e.getKey(), ((AtomAnnotation)e.getValue()).getAtom());
        }
        t2atom = new SoftReference<>(map);
        return map;
    }

    public @Nonnull Set<Atom> termAtoms(@Nonnull Term term) {
        return termAtoms().get(term);
    }

    /**
     * A join connected query is in which any triple can be reached from another triple
     * through joins between triples sharing at least one variable.
     *
     * @return true iff this query is join connected
     */
    public boolean isJoinConnected() {
        if (joinConnected == null)
            joinConnected = verifyJoinConnected();
        return joinConnected;
    }

    /**
     * If a query has been determined to be join-connected externally (e.g., OuterPlanner),
     * then this setter may be called to avoid recomputing connectedness.
     */
    public void setJoinConnected(boolean joinConnected) {
        assert this.joinConnected == null || this.joinConnected == joinConnected;
        assert this.joinConnected != null || verifyJoinConnected() == joinConnected;
        this.joinConnected = joinConnected;
    }

    private boolean verifyJoinConnected() {
        if (d.list.isEmpty()) return true;

        IndexSet<Triple> triples = getSet();
        IndexSubset<Triple> visited = triples.emptySubset();
        ArrayDeque<Triple> queue = new ArrayDeque<>();
        queue.add(triples.get(0));
        while (!queue.isEmpty()) {
            Triple triple = queue.remove();
            if (!visited.add(triple)) continue;
            triple.forEach(t -> {
                if (t.isVar())
                    queue.addAll(triplesWithTerm(t));
            });
        }
        return visited.equals(triples);
    }

    public boolean isAsk() {
        if (ask == null)
            ask = d.modifiers.ask() != null || publicVarNames().isEmpty();
        return ask;
    }

    /** All terms are bound, either because <code>isAsk()</code> or because it is empty. */
    public boolean allBound() {
        return isAsk() || d.list.isEmpty();
    }

    /**
     * Get the limit value of the modifier, if defined.
     *
     * @return {@link Limit#getValue()} if defined or zero otherwise
     */
    public int limit() {
        if (this.limit >= 0)
            return this.limit;
        Limit limit = ModifierUtils.getFirst(Limit.class, d.modifiers);
        return this.limit = limit == null ? 0 : limit.getValue();
    }

    public int queryHash() {
        if (queryHash != 0)
            return queryHash;
        int code = d.list.hashCode();
        code = 37*code + d.modifiers.hashCode();
        code = 37*code + d.queryAnns.hashCode();
        code = 37*code + d.termAnns.hashCode();
        code = 37*code + d.tripleAnns.hashCode();
        return queryHash = code;
    }
}
