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
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ImmFullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSetPartition;
import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.SimpleImmIndexSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.HashMultimap;
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

    private IndexSet<Triple> set;
    private IndexSet<Term> allTerms;
    private IndexSetPartition<Term> tripleTerms;
    private ImmIndexSubset<Var> allVars;
    private ImmIndexSubset<Var> tripleVars;
    private IndexSet<String> varNamesUniverse;
    private IndexSet<String> allVarNames;
    private IndexSetPartition<String> tripleVarNames;
    private ImmIndexSubset<String> publicTripleVarNames;
    private ImmIndexSubset<String> publicVarNames;
    private ImmIndexSubset<String> inputVarNames, reqInputVarNames, optInputVarNames;

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

    public @Nonnull IndexSet<Triple> getSet() {
        if (set == null)
            set = FullIndexSet.from(d.list);
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
        this.allTerms = allTerms;
        tripleTerms = IndexSetPartition.of(this.allTerms, 0, tripleTermsCount);
    }

    public @Nonnull IndexSet<Term> allTerms() {
        if (allTerms == null)
            indexTerms();
        return allTerms;
    }

    public @Nonnull ImmIndexSubset<Var> allVars() {
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

    public @Nonnull IndexSetPartition<String> tripleVarNames() {
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
        Projection projection = d.modifiers.projection();
        if (projection == null) {
            allVarNames = varNamesUniverse = univ;
        } else {
            int allVarsEnd = univ.size();
            boolean added = univ.addAll(projection.getVarNames());
            varNamesUniverse = univ;
            allVarNames = added ? IndexSetPartition.of(univ, 0, allVarsEnd) : univ;
        }
        tripleVarNames = IndexSetPartition.of(varNamesUniverse, 0, tripleVarsEnd);
    }

    public @Nonnull ImmIndexSubset<String> publicVarNames() {
        if (publicVarNames == null) {
            if (d.modifiers.ask() != null) {
                IndexSet<String> set = ImmFullIndexSet.empty();
                return publicVarNames = set.immutableEmptySubset();
            }
            Projection projection = d.modifiers.projection();
            if (projection == null)
                return publicVarNames = varNamesUniverse().immutableFullSubset();
            if (varNamesUniverse == null)
                return varNamesUniverse().immutableSubset(projection.getVarNames());

            BitSet bs = new BitSet(varNamesUniverse.size());
            for (String name : projection.getVarNames()) {
                int idx = varNamesUniverse.indexOf(name);
                if (idx < 0) {
                    idx = varNamesUniverse.size();
                    varNamesUniverse.add(name);
                    assert idx == varNamesUniverse.indexOf(name);
                }
                bs.set(idx);
            }
            publicVarNames = new SimpleImmIndexSubset<>(varNamesUniverse, bs);
            assert varNamesUniverse.containsAll(projection.getVarNames());
            assert publicVarNames.containsAll(projection.getVarNames());
        }
        return publicVarNames;
    }

    public @Nonnull ImmIndexSubset<String> publicTripleVarNames() {
        if (publicTripleVarNames == null) {
            ImmIndexSubset<String> pub = publicVarNames();
            BitSet bs = (BitSet) pub.getBitSet().clone();
            int tripleVarNamesEnd = tripleVarNames().getEnd();
            assert tripleVarNamesEnd <= varNamesUniverse.size();
            int size = bs.size();
            if (size > tripleVarNamesEnd) bs.clear(tripleVarNamesEnd, size);
            publicTripleVarNames = new SimpleImmIndexSubset<>(allVarNames(), bs);
        }
        return publicTripleVarNames;
    }

    private void scanInputs() {
        IndexSet<String> allVarNames = allVarNames();
        IndexSubset<String> req = allVarNames.emptySubset(), opt = allVarNames.emptySubset();
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
            if (!opt.hasIndex(idx, allVarNames) && !tripleVarNames.contains(e.getKey()))
                req.setIndex(idx, allVarNames); //FILTER input var
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
        if (matchedTriples != null)
            return matchedTriples;
        FullIndexSet<Triple> set = new FullIndexSet<>(d.list.size());
        for (Triple triple : d.list) {
            boolean has = false;
            for (TripleAnnotation a : d.tripleAnns.get(triple)) {
                if (a instanceof MatchAnnotation) {
                    has = true;
                    Triple matched = ((MatchAnnotation) a).getMatched();
                    set.add(matched);
                }
            }
            if (!has)
                set.add(triple);
        }
        return matchedTriples = set;
    }

    private void indexTriples() {
        if (!indexTriplesState.compareAndSet(0, 1)) {
            // already initialized or other thread is initializing, spinlock
            while (indexTriplesState.get() != 2)
                Thread.yield();
            return;
        }
        IndexSet<Triple> triples = getSet();
        IndexSet<Term> terms = tripleTerms();
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

        s2triple = toImmIndexedSubset(triples, sSets);
        p2triple = toImmIndexedSubset(triples, pSets);
        o2triple = toImmIndexedSubset(triples, oSets);
        indexTriplesState.set(2);
    }

    private static final @Nonnull BitSet EMPTY_BITSET = new BitSet();

    private static @Nonnull <T> SoftReference<List<ImmIndexSubset<T>>>
    toImmIndexedSubset(@Nonnull IndexSet<T> parent, @Nonnull BitSet[] sets) {
        List<ImmIndexSubset<T>> list = new ArrayList<>(sets.length);
        for (BitSet set : sets)
            list.add(new SimpleImmIndexSubset<>(parent, set == null ? EMPTY_BITSET : set));
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
        return new SimpleImmIndexSubset<>(triples, set);
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

    private Boolean verifyJoinConnected() {
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
