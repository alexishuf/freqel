package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.query.annotations.MatchAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TermAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.LongBitset;
import br.ufsc.lapesd.freqel.util.indexed.*;
import br.ufsc.lapesd.freqel.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.freqel.util.indexed.subset.SimpleImmIndexSubset;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static br.ufsc.lapesd.freqel.util.bitset.Bitsets.createFixed;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class CQueryCache {
    private @Nonnull final CQueryData d;
    private List<Triple> unmodifiableList;
    private Set<QueryAnnotation> unmodifiableQueryAnnotations;

    private IndexSet<Triple> set;
    private IndexSet<Term> allTerms;
    private IndexSetPartition<Term> tripleTerms;
    private IndexSubset<Var> allVars;
    private IndexSubset<Var> tripleVars;
    private IndexSet<Triple> offeredTriplesUniverse;
    private IndexSet<String> varNamesUniverse, offeredVarNamesUniverse;
    private IndexSet<String> allVarNames;
    private IndexSet<String> tripleVarNames;
    private IndexSubset<String> publicTripleVarNames;
    private IndexSet<String> publicVarNames;
    private IndexSubset<String> inputVarNames, reqInputVarNames, optInputVarNames;

    private final AtomicInteger indexTriplesState = new AtomicInteger(0);
    private SoftReference<List<ImmIndexSubset<Triple>>> s2triple, p2triple, o2triple;
    private SoftReference<SetMultimap<Term, Atom>> t2atom;
    private IndexSet<Triple> matchedTriples;

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
            if (offeredTriplesUniverse != null) {
                Bitset bs = createFixed(offeredTriplesUniverse.size());
                for (Triple triple : d.list) {
                    int idx = offeredTriplesUniverse.indexOf(triple);
                    if (idx < 0) // do it the expensive way
                        return set = offeredTriplesUniverse.immutableSubsetExpanding(d.list);
                    bs.set(idx);
                }
                set = offeredTriplesUniverse.immutableSubset(bs);
            } else {
                set = FullIndexSet.from(d.list);
            }
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
            indexTripleTerms();
        return tripleTerms;
    }

    public @Nonnull IndexSubset<Var> tripleVars() {
        if (tripleVars == null) {
            IndexSet<Term> terms = tripleTerms();
            Bitset bs = createFixed(terms.size());
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

    private void indexTripleTerms() {
        int capacity = d.list.size()*3 + d.modifiers.filters().size()*2;
        FullIndexSet<Term> allTerms = new FullIndexSet<>(capacity);
        for (Triple triple : d.list) {
            allTerms.add(triple.getSubject());
            allTerms.add(triple.getPredicate());
            allTerms.add(triple.getObject());
        }
        this.allTerms = allTerms;
        tripleTerms = IndexSetPartition.of(this.allTerms, 0, allTerms.size());
    }

    private void indexTerms() {
        if (tripleTerms == null) {
            indexTripleTerms();
        } else if (allTerms == null) {
            // only the filter terms were invalidated. Erase them and restore allTerms
            FullIndexSet<Term> parent = (FullIndexSet<Term>)tripleTerms.getParent();
            parent.removeTail(parent.size()-tripleTerms.size());
            allTerms = parent;
        } else if (tripleTerms.size() < allTerms.size()) {
            return; // already indexed filters
        }
        for (SPARQLFilter filter : d.modifiers.filters()) {
            allTerms.addAll(filter.getTerms());
        }
    }

    public @Nonnull IndexSet<Term> allTerms() {
        indexTerms();
        return allTerms;
    }

    public @Nonnull IndexSet<Var> allVars() {
        if (allVars == null) {
            IndexSet<Term> terms = allTerms();
            Bitset bs = tripleVars().getBitset().copy();
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

    public @Nonnull IndexSet<String> tripleVarNames() {
        if (tripleVarNames == null)
            indexVarNames();
        return tripleVarNames;
    }

    public @Nonnull IndexSet<String> allVarNames() {
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
        this.allVarNames = allVarNames;
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

    public @Nonnull IndexSet<String> publicVarNames() {
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
            publicVarNames = subset;
            assert varNamesUniverse.containsAll(projection.getVarNames());
            assert publicVarNames.containsAll(projection.getVarNames());
        }
        return publicVarNames;
    }

    public @Nonnull IndexSubset<String> publicTripleVarNames() {
        if (publicTripleVarNames == null) {
            publicTripleVarNames = publicVarNames().fullSubset().intersect(tripleVarNames());
        }
        return publicTripleVarNames;
    }

    private void scanInputs() {
        IndexSet<String> allNames = allVarNames();
        IndexSet<String> parent = allNames instanceof IndexSubset ? allNames.getParent() : allNames;
        Bitset req = createFixed(parent.size()), opt = createFixed(parent.size());
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
                (ia.isRequired() ? req : opt).set(parent.indexOf(name));
            }
        }
        for (Iterator<Map.Entry<String, Integer>> it = allNames.entryIterator(); it.hasNext(); ){
            Map.Entry<String, Integer> e = it.next();
            int idx = e.getValue();
            if (!opt.get(idx) && !tripleVarNames.contains(e.getKey()))
                req.set(idx); //FILTER input var
        }

        reqInputVarNames = parent.immutableSubset(req);
        optInputVarNames = parent.immutableSubset(opt);
    }

    public @Nonnull IndexSubset<String> inputVarNames() {
        if (inputVarNames == null) {
            IndexSubset<String> set = allVarNames().emptySubset();
            set.addAll(reqInputVarNames());
            set.addAll(optInputVarNames());
            inputVarNames = set;
        }
        return inputVarNames;
    }
    public @Nonnull IndexSubset<String> reqInputVarNames() {
        if (reqInputVarNames == null)
            scanInputs();
        return reqInputVarNames;
    }
    public @Nonnull IndexSubset<String> optInputVarNames() {
        if (optInputVarNames == null)
            scanInputs();
        return optInputVarNames;
    }

    public @Nonnull IndexSet<Triple> matchedTriples() {
        if (matchedTriples == null) {
            if (d.tripleAnns.isEmpty())
                return matchedTriples = getSet();
            if (offeredTriplesUniverse != null) {
                try {
                    return matchedTriples = fillMatched(offeredTriplesUniverse.emptySubset());
                } catch (NotInParentException ignored) {  }
            }
            matchedTriples = fillMatched(new FullIndexSet<>(d.list.size()));
        }
        return matchedTriples;
    }

    private @Nonnull IndexSet<Triple> fillMatched(@Nonnull IndexSet<Triple> set) {
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
        return set;
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
        int termCount = terms.size(), ntriples = d.list.size();
        Bitset[] sSets = new Bitset[termCount], pSets = new Bitset[termCount],
                 oSets = new Bitset[termCount];
        if (triples instanceof IndexSubset) {
            for (Triple t : d.list) {
                int sIdx = terms.indexOf(t.getSubject()), oIdx = terms.indexOf(t.getObject());
                int pIdx = terms.indexOf(t.getPredicate()), i = triplesUniverse.indexOf(t);
                (sSets[sIdx] == null ? sSets[sIdx] = createFixed(ntriples) : sSets[sIdx]).set(i);
                (pSets[pIdx] == null ? pSets[pIdx] = createFixed(ntriples) : pSets[pIdx]).set(i);
                (oSets[oIdx] == null ? oSets[oIdx] = createFixed(ntriples) : oSets[oIdx]).set(i);
            }
        } else {
            assert new ArrayList<>(triples).equals(d.list);
            for (int i = 0; i < ntriples; i++) {
                Triple t = d.list.get(i);
                int sIdx = terms.indexOf(t.getSubject()), oIdx = terms.indexOf(t.getObject());
                int pIdx = terms.indexOf(t.getPredicate());
                (sSets[sIdx] == null ? sSets[sIdx] = createFixed(ntriples) : sSets[sIdx]).set(i);
                (pSets[pIdx] == null ? pSets[pIdx] = createFixed(ntriples) : pSets[pIdx]).set(i);
                (oSets[oIdx] == null ? oSets[oIdx] = createFixed(ntriples) : oSets[oIdx]).set(i);
            }
        }
        s2triple = toImmIndexedSubset(triplesUniverse, sSets);
        p2triple = toImmIndexedSubset(triplesUniverse, pSets);
        o2triple = toImmIndexedSubset(triplesUniverse, oSets);
        indexTriplesState.set(2);
    }

    private static final @Nonnull Bitset EMPTY_BITSET = LongBitset.EMPTY;

    private static @Nonnull <T> SoftReference<List<ImmIndexSubset<T>>>
    toImmIndexedSubset(@Nonnull IndexSet<T> parent, @Nonnull Bitset[] sets) {
        List<ImmIndexSubset<T>> list = new ArrayList<>(sets.length);
        for (Bitset set : sets)
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
        Bitset set = createFixed(triples.getParent().size());
        set.or(s2t.get(idx).getBitset());
        set.or(p2t.get(idx).getBitset());
        set.or(o2t.get(idx).getBitset());
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
