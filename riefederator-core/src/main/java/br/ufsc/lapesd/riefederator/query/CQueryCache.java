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
import br.ufsc.lapesd.riefederator.util.ImmutableIndexedSubset;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSetPartition;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class CQueryCache {
    private @Nonnull final CQueryData d;
    private List<Triple> unmodifiableList;
    private Set<QueryAnnotation> unmodifiableQueryAnnotations;

    private IndexedSet<Triple> set;
    private IndexedSet<Term> allTerms;
    private IndexedSetPartition<Term> tripleTerms;
    private ImmutableIndexedSubset<Var> allVars;
    private ImmutableIndexedSubset<Var> tripleVars;
    private IndexedSet<String> varNamesUniverse;
    private IndexedSet<String> allVarNames;
    private IndexedSetPartition<String> tripleVarNames;
    private ImmutableIndexedSubset<String> publicTripleVarNames;
    private ImmutableIndexedSubset<String> publicVarNames;
    private ImmutableIndexedSubset<String> inputVarNames, reqInputVarNames, optInputVarNames;

    private final AtomicInteger indexTriplesState = new AtomicInteger(0);
    private SoftReference<List<ImmutableIndexedSubset<Triple>>> s2triple, p2triple, o2triple;
    private SoftReference<SetMultimap<Term, Atom>> t2atom;
    private IndexedSet<Triple> matchedTriples;

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
        publicTripleVarNames = publicVarNames = null;
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

    public @Nonnull IndexedSet<Triple> getSet() {
        if (set == null)
            set = IndexedSet.from(d.list);
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

    public @Nonnull IndexedSet<Term> tripleTerms() {
        if (tripleTerms == null)
            indexTerms();
        return tripleTerms;
    }

    public @Nonnull ImmutableIndexedSubset<Var> tripleVars() {
        if (tripleVars == null) {
            IndexedSet<Term> terms = tripleTerms();
            BitSet bs = new BitSet(terms.size());
            int i = 0;
            for (Term term : terms) {
                if (term.isVar())
                    bs.set(i);
                ++i;
            }
            @SuppressWarnings("UnnecessaryLocalVariable") IndexedSet<?> erased = terms;
            //noinspection unchecked
            tripleVars = new ImmutableIndexedSubset<>((IndexedSet<Var>) erased, bs);
        }
        return tripleVars;
    }

    private void indexTerms() {
        int capacity = d.list.size()*3 + d.modifiers.filters().size()*2;
        Map<Term, Integer> map = Maps.newHashMapWithExpectedSize(capacity);
        List<Term> list = new ArrayList<>(capacity);
        for (Triple t : d.list) {
            Term s = t.getSubject(), p = t.getPredicate(), o = t.getObject();
            if (map.putIfAbsent(s, list.size()) == null) list.add(s);
            if (map.putIfAbsent(p, list.size()) == null) list.add(p);
            if (map.putIfAbsent(o, list.size()) == null) list.add(o);
        }
        int tripleTermsCount = list.size();
        for (SPARQLFilter filter : d.modifiers.filters()) {
            for (Term term : filter.getTerms())
                if (map.putIfAbsent(term, list.size()) == null) list.add(term);
        }
        allTerms = IndexedSet.fromMap(map, list);
        BitSet bs = new BitSet();
        bs.set(0, tripleTermsCount);
        tripleTerms = allTerms.partition(0, tripleTermsCount);
    }

    public @Nonnull IndexedSet<Term> allTerms() {
        if (allTerms == null)
            indexTerms();
        return allTerms;
    }

    public @Nonnull ImmutableIndexedSubset<Var> allVars() {
        if (allVars == null) {
            IndexedSet<Term> terms = allTerms();
            BitSet bs = (BitSet) tripleVars().getBitSet().clone();
            for (int i = tripleTerms.getEnd(); i < terms.size(); i++) {
                Term term = terms.get(i);
                if (term.isVar())
                    bs.set(i);
            }
            @SuppressWarnings("UnnecessaryLocalVariable") IndexedSet<?> cleared = terms;
            //noinspection unchecked
            allVars = new ImmutableIndexedSubset<>((IndexedSet<Var>) cleared, bs);
        }
        return allVars;
    }

    public @Nonnull IndexedSetPartition<String> tripleVarNames() {
        if (tripleVarNames == null)
            indexVarNames();
        return tripleVarNames;
    }

    public @Nonnull IndexedSet<String> allVarNames() {
        if (allVarNames == null)
            indexVarNames();
        return allVarNames;
    }

    private @Nonnull IndexedSet<String> varNamesUniverse() {
        if (varNamesUniverse == null)
            indexVarNames();
        return varNamesUniverse;
    }

    private void indexVarNames() {
        IndexedSet<Term> terms = allTerms();
        int capacity = d.list.size()*2, size = 0;
        Map<String, Integer> map = new HashMap<>(capacity);
        List<String> list = new ArrayList<>(capacity);
        int tripleVarsEnd = -1, termsIdx  = -1, tripleTermsEnd = tripleTerms.getEnd();
        for (Term term : terms) {
            ++termsIdx;
            if (term.isVar()) {
                if (termsIdx >= tripleTermsEnd && tripleVarsEnd < 0)
                    tripleVarsEnd = list.size();
                String name = term.asVar().getName();
                map.put(name, size++);
                list.add(name);
            }
        }
        assert map.size() == size;
        assert size == list.size();
        if (tripleVarsEnd < 0)
            tripleVarsEnd = list.size();
        Projection projection = d.modifiers.projection();
        if (projection == null) {
            allVarNames = varNamesUniverse = IndexedSet.fromMap(map, list);
        } else {
            int allVarsEnd = size;
            for (String name : projection.getVarNames()) {
                if (map.putIfAbsent(name, size) == null) {
                    list.add(name);
                    ++size;
                }
            }
            assert map.size() == size;
            assert size == list.size();
            varNamesUniverse = IndexedSet.fromMap(map, list);
            allVarNames = map.size() == allVarsEnd ? varNamesUniverse
                                                   : varNamesUniverse.partition(0, allVarsEnd);
        }
        tripleVarNames = allVarNames.partition(0, tripleVarsEnd);
    }

    public @Nonnull ImmutableIndexedSubset<String> publicVarNames() {
        if (publicVarNames == null) {
            if (d.modifiers.ask() != null) {
                IndexedSet<String> set = IndexedSet.empty();
                return publicVarNames = set.immutableEmptySubset();
            }
            Projection projection = d.modifiers.projection();
            if (projection == null)
                return publicVarNames = varNamesUniverse().fullImmutableSubset();
            if (varNamesUniverse == null)
                return varNamesUniverse().immutableSubset(projection.getVarNames());

            BitSet bs = new BitSet();
            boolean failed = false;
            for (String name : projection.getVarNames()) {
                int idx = varNamesUniverse.indexOf(name);
                if ((failed = idx < 0)) break;
                else                    bs.set(idx);
            }
            if (failed) {
                varNamesUniverse = varNamesUniverse.createMutation((m, l) -> {
                    int size = l.size();
                    for (String name : projection.getVarNames()) {
                        int idx = m.getOrDefault(name, -1);
                        if (idx < 0) {
                            m.put(name, size);
                            l.add(name);
                            bs.set(size++);
                        } else {
                            bs.set(idx);
                        }
                    }
                    assert size == l.size();
                });
                assert varNamesUniverse.containsAll(projection.getVarNames());
            }
            publicVarNames = new ImmutableIndexedSubset<>(varNamesUniverse, bs);
            assert publicVarNames.containsAll(projection.getVarNames());
        }
        return publicVarNames;
    }

    public @Nonnull ImmutableIndexedSubset<String> publicTripleVarNames() {
        if (publicTripleVarNames == null) {
            ImmutableIndexedSubset<String> pub = publicVarNames();
            BitSet bs = (BitSet) pub.getBitSet().clone();
            int tripleVarNamesEnd = tripleVarNames().getEnd();
            assert tripleVarNamesEnd <= varNamesUniverse.size();
            int size = bs.size();
            if (size > tripleVarNamesEnd) bs.clear(tripleVarNamesEnd, size);
            publicTripleVarNames = new ImmutableIndexedSubset<>(allVarNames(), bs);
        }
        return publicTripleVarNames;
    }

    private void scanInputs() {
        IndexedSet<String> allVarNames = allVarNames();
        IndexedSubset<String> req = allVarNames.emptySubset(), opt = allVarNames.emptySubset();
        IndexedSet<String> tripleVarNames = tripleVarNames();
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
            if (!opt.hasIndex(idx, allVarNames) && !tripleVarNames.contains(e.getKey()))
                req.setIndex(idx, allVarNames); //FILTER input var
        }
        reqInputVarNames = req.toImmutable();
        optInputVarNames = opt.toImmutable();
    }

    public @Nonnull ImmutableIndexedSubset<String> inputVarNames() {
        if (inputVarNames == null) {
            IndexedSubset<String> set = allVarNames().emptySubset();
            set.addAll(reqInputVarNames());
            set.addAll(optInputVarNames());
            inputVarNames = ImmutableIndexedSubset.copyOf(set);
        }
        return inputVarNames;
    }
    public @Nonnull ImmutableIndexedSubset<String> reqInputVarNames() {
        if (reqInputVarNames == null)
            scanInputs();
        return reqInputVarNames;
    }
    public @Nonnull ImmutableIndexedSubset<String> optInputVarNames() {
        if (optInputVarNames == null)
            scanInputs();
        return optInputVarNames;
    }

    public @Nonnull IndexedSet<Triple> matchedTriples() {
        if (matchedTriples != null)
            return matchedTriples;
        Map<Triple, Integer> map = new HashMap<>();
        List<Triple> list = new ArrayList<>(d.list.size());
        for (Triple triple : d.list) {
            boolean has = false;
            for (TripleAnnotation a : d.tripleAnns.get(triple)) {
                if (a instanceof MatchAnnotation) {
                    has = true;
                    Triple matched = ((MatchAnnotation) a).getMatched();
                    if (map.putIfAbsent(matched, list.size()) == null)
                        list.add(matched);
                }
            }
            if (!has) {
                if (map.putIfAbsent(triple, list.size()) == null)
                    list.add(triple);
            }
        }
        return matchedTriples = IndexedSet.fromMap(map, list);
    }

    private void indexTriples() {
        if (!indexTriplesState.compareAndSet(0, 1)) {
            // already initialized or other thread is initializing, spinlock
            while (indexTriplesState.get() != 2)
                Thread.yield();
            return;
        }
        IndexedSet<Triple> triples = getSet();
        IndexedSet<Term> terms = tripleTerms();
        int termCount = terms.size(), tripleCount = d.list.size();
        BitSet[] sSets = new BitSet[termCount], pSets = new BitSet[termCount],
                 oSets = new BitSet[termCount];
        for (int i = 0; i < tripleCount; i++) {
            Triple triple = d.list.get(i);
            int sIdx = terms.indexOf(triple.getSubject()), oIdx = terms.indexOf(triple.getObject());
            int pIdx = terms.indexOf(triple.getPredicate());
            (sSets[sIdx] == null ? sSets[sIdx] = new BitSet(tripleCount) : sSets[sIdx]).set(i);
            (pSets[pIdx] == null ? pSets[pIdx] = new BitSet(tripleCount) : pSets[pIdx]).set(i);
            (oSets[oIdx] == null ? oSets[oIdx] = new BitSet(tripleCount) : oSets[oIdx]).set(i);
        }

        s2triple = toImmutableIndexedSubset(triples, sSets);
        p2triple = toImmutableIndexedSubset(triples, pSets);
        o2triple = toImmutableIndexedSubset(triples, oSets);
        indexTriplesState.set(2);
    }

    private static final @Nonnull BitSet EMPTY_BITSET = new BitSet();

    private static @Nonnull <T> SoftReference<List<ImmutableIndexedSubset<T>>>
    toImmutableIndexedSubset(@Nonnull IndexedSet<T> parent, @Nonnull BitSet[] sets) {
        List<ImmutableIndexedSubset<T>> list = new ArrayList<>(sets.length);
        for (BitSet set : sets)
            list.add(new ImmutableIndexedSubset<>(parent, set == null ? EMPTY_BITSET : set));
        return new SoftReference<>(list);
    }

    public @Nonnull ImmutableIndexedSubset<Triple> triplesWithTerm(@Nonnull Term term) {
        IndexedSet<Triple> triples = getSet();
        int idx = tripleTerms().indexOf(term);
        if (idx < 0)
            return triples.immutableEmptySubset();
        List<ImmutableIndexedSubset<Triple>> s2t, p2t, o2t;
        while ((s2t = s2triple == null ? null : s2triple.get()) == null) indexTriples();
        while ((p2t = p2triple == null ? null : p2triple.get()) == null) indexTriples();
        while ((o2t = o2triple == null ? null : o2triple.get()) == null) indexTriples();
        BitSet set = new BitSet();
        set.or(s2t.get(idx).getBitSet());
        set.or(p2t.get(idx).getBitSet());
        set.or(o2t.get(idx).getBitSet());
        return new ImmutableIndexedSubset<>(triples, set);
    }

    public @Nonnull IndexedSubset<Triple> triplesWithTermAt(@Nonnull Term term,
                                                            @Nonnull Triple.Position position) {
        int termIdx = tripleTerms().indexOf(term);
        if (termIdx < 0)
            return getSet().immutableEmptySubset();
        List<ImmutableIndexedSubset<Triple>> list = null;
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

    private Boolean verifyJoinConnected() {
        if (d.list.isEmpty()) return true;

        IndexedSet<Triple> triples = getSet();
        IndexedSubset<Triple> visited = triples.emptySubset();
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
