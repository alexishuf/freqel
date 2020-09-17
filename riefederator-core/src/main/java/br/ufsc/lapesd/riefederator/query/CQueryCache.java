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
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class CQueryCache {
    private @Nonnull final CQueryData d;
    private List<Triple> unmodifiableList;
    private Set<QueryAnnotation> unmodifiableQueryAnnotations;

    private IndexedSet<Triple> set;
    private IndexedSet<Term> tripleTerms;
    private IndexedSet<Term> allTerms;
    private IndexedSet<Var> tripleVars;
    private IndexedSet<Var> allVars;
    private IndexedSet<String> tripleVarNames;
    private IndexedSet<String> allVarNames;
    private IndexedSet<String> publicTripleVarNames;
    private IndexedSet<String> publicVarNames;
    private ImmutableIndexedSubset<String> inputVarNames, reqInputVarNames, optInputVarNames;

    private final AtomicInteger indexTriplesState = new AtomicInteger(0);
    private List<ImmutableIndexedSubset<Triple>> s2triple, p2triple, o2triple;
    private SetMultimap<Term, Atom> t2atom;
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

    void invalidateTriples() {
        set = null;
        unmodifiableList = null;
        tripleTerms = null;
        allTerms = null;
        tripleVars = allVars = null;
        tripleVarNames = allVarNames = null;
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
            unmodifiableQueryAnnotations = unmodifiableSet(d.queryAnnotations);
        return unmodifiableQueryAnnotations;
    }

    public @Nonnull IndexedSet<Term> tripleTerms() {
        if (tripleTerms == null) {
            int capacity = d.list.size() * 2;
            List<Term> list = new ArrayList<>(capacity);
            HashMap<Term, Integer> map = new HashMap<>(capacity);
            for (Triple t : d.list) {
                Term s = t.getSubject(), p = t.getPredicate(), o = t.getObject();
                if (map.putIfAbsent(s, list.size()) == null) list.add(s);
                if (map.putIfAbsent(p, list.size()) == null) list.add(p);
                if (map.putIfAbsent(o, list.size()) == null) list.add(o);
            }
            tripleTerms = IndexedSet.fromMap(map, list);
        }
        return tripleTerms;
    }

    public @Nonnull IndexedSet<Var> tripleVars() {
        if (tripleVars == null) {
            IndexedSet<Term> terms = tripleTerms();
            List<Var> list = new ArrayList<>(terms.size());
            for (Term term : terms) {
                if (term.isVar())
                    list.add(term.asVar());
            }
            tripleVars = IndexedSet.fromDistinct(list);
        }
        return tripleVars;
    }

    public @Nonnull IndexedSet<Term> allTerms() {
        if (allTerms != null)
            return allTerms;
        Set<SPARQLFilter> filters = d.modifiers.filters();
        IndexedSet<Term> tripleTerms = tripleTerms();
        int capacity = tripleTerms.size() + 2*filters.size();
        ArrayList<Term> list = new ArrayList<>(capacity);
        HashMap<Term, Integer> map = Maps.newHashMapWithExpectedSize(capacity);
        list.addAll(tripleTerms);
        map.putAll(tripleTerms.getPositionsMap());
        for (SPARQLFilter filter : filters) {
            for (Term term : filter.getTerms()) {
                if (map.putIfAbsent(term, list.size()) == null)
                    list.add(term);
            }
        }
        return allTerms = IndexedSet.fromMap(map, list);
    }

    public @Nonnull IndexedSet<Var> allVars() {
        if (allVars != null)
            return allVars;
        Set<SPARQLFilter> filters = d.modifiers.filters();
        IndexedSet<Var> tripleVars = tripleVars();
        int capacity = tripleVars.size() + filters.size();
        List<Var> list = new ArrayList<>(capacity);
        Map<Var, Integer> map = Maps.newHashMapWithExpectedSize(capacity);
        list.addAll(tripleVars);
        map.putAll(tripleVars.getPositionsMap());
        for (SPARQLFilter filter : filters) {
            for (Var var : filter.getVarTerms()) {
                if (map.putIfAbsent(var, list.size()) == null)
                    list.add(var);
            }
        }
        return allVars = IndexedSet.fromMap(map, list);
    }

    public @Nonnull IndexedSet<String> tripleVarNames() {
        if (tripleVarNames == null) {
            IndexedSet<Var> vars = tripleVars();
            ArrayList<String> list = new ArrayList<>(vars.size());
            for (Var var : vars)
                list.add(var.getName());
            tripleVarNames = IndexedSet.fromDistinct(list);
        }
        return tripleVarNames;
    }

    public @Nonnull IndexedSet<String> allVarNames() {
        if (allVarNames == null) {
            IndexedSet<Var> vars = allVars();
            List<String> list = new ArrayList<>(vars.size());
            for (Var var : vars)
                list.add(var.getName());
            allVarNames = IndexedSet.fromDistinct(list);
        }
        return allVarNames;
    }

    public @Nonnull IndexedSet<String> publicVarNames() {
        if (publicVarNames != null)
            return publicVarNames;
        if (isAsk())
            return publicVarNames = IndexedSet.empty();
        Projection projection = d.modifiers.projection();
        if (projection == null)
            return publicVarNames = allVarNames();
        return publicVarNames = IndexedSet.fromDistinct(projection.getVarNames());
    }

    public @Nonnull IndexedSet<String> publicTripleVarNames() {
        if (publicTripleVarNames != null)
            return publicTripleVarNames;
        if (isAsk())
            return publicTripleVarNames = IndexedSet.empty();
        IndexedSet<String> pub = publicVarNames();
        IndexedSet<String> prv = tripleVarNames();
        Map<String, Integer> map = Maps.newHashMapWithExpectedSize(pub.size());
        List<String> list = new ArrayList<>();
        for (String var : prv) {
            if (pub.contains(var)) {
                Integer old = map.putIfAbsent(var, list.size());
                assert old == null;
                list.add(var);
            }
        }
        return publicTripleVarNames = IndexedSet.fromMap(map, list);
    }

    private @Nonnull ImmutableIndexedSubset<String> scanInputs(boolean required) {
        IndexedSubset<String> set = allVarNames().emptySubset();
        IndexedSet<String> tripleVarNames = tripleVarNames();
        for (Var v : allVars()) {
            String name = v.getName();
            boolean hasAnnotation = false;
            for (TermAnnotation a : d.termAnnotations.get(v)) {
                if (!(a instanceof AtomInputAnnotation))
                    continue;
                hasAnnotation = true;
                AtomInputAnnotation ia = (AtomInputAnnotation) a;
                if (ia.isOverride() && requireNonNull(ia.getOverrideValue()).isGround())
                    continue; //not a input anymore
                if (ia.isRequired() == required)
                    set.add(name);
            }
            if (required && !hasAnnotation && !tripleVarNames.contains(name))
                set.add(name); //FILTER input var
        }
        return ImmutableIndexedSubset.copyOf(set);
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
            reqInputVarNames = scanInputs(true);
        return reqInputVarNames;
    }
    public @Nonnull ImmutableIndexedSubset<String> optInputVarNames() {
        if (optInputVarNames == null)
            optInputVarNames = scanInputs(false);
        return optInputVarNames;
    }

    public @Nonnull IndexedSet<Triple> matchedTriples() {
        if (matchedTriples != null)
            return matchedTriples;
        Map<Triple, Integer> map = new HashMap<>();
        List<Triple> list = new ArrayList<>(d.list.size());
        for (Triple triple : d.list) {
            boolean has = false;
            for (TripleAnnotation a : d.tripleAnnotations.get(triple)) {
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

    private static @Nonnull <T> List<ImmutableIndexedSubset<T>>
    toImmutableIndexedSubset(@Nonnull IndexedSet<T> parent, @Nonnull BitSet[] sets) {
        List<ImmutableIndexedSubset<T>> list = new ArrayList<>(sets.length);
        for (BitSet set : sets)
            list.add(new ImmutableIndexedSubset<>(parent, set == null ? EMPTY_BITSET : set));
        return list;
    }

    public @Nonnull ImmutableIndexedSubset<Triple> triplesWithTerm(@Nonnull Term term) {
        IndexedSet<Triple> triples = getSet();
        int idx = tripleTerms().indexOf(term);
        if (idx < 0)
            return triples.immutableEmptySubset();
        indexTriples();
        BitSet set = new BitSet();
        set.or(s2triple.get(idx).getBitSet());
        set.or(p2triple.get(idx).getBitSet());
        set.or(o2triple.get(idx).getBitSet());
        return new ImmutableIndexedSubset<>(triples, set);
    }

    public @Nonnull IndexedSubset<Triple> triplesWithTermAt(@Nonnull Term term,
                                                            @Nonnull Triple.Position position) {
        int termIdx = tripleTerms().indexOf(term);
        if (termIdx < 0)
            return getSet().immutableEmptySubset();
        indexTriples();
        List<ImmutableIndexedSubset<Triple>> list;
        switch (position) {
            case SUBJ:
                list = s2triple; break;
            case PRED:
                list = p2triple; break;
            case OBJ:
                list = o2triple; break;
            default:
                throw new IllegalArgumentException("Bad position="+position);
        }
        return list.get(termIdx);
    }

    public @Nonnull SetMultimap<Term, Atom> termAtoms() {
        if (t2atom != null)
            return t2atom;
        HashMultimap<Term, Atom> map = HashMultimap.create(tripleTerms().size(), 2);
        for (Map.Entry<Term, TermAnnotation> e : d.termAnnotations.entries()) {
            if (e.getValue() instanceof AtomAnnotation)
                map.put(e.getKey(), ((AtomAnnotation)e.getValue()).getAtom());
        }
        return t2atom = map;
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
        if (ask != null)
            return ask;
        return ask = d.modifiers.ask() != null
                || (!d.list.isEmpty() && d.list.stream().allMatch(Triple::isBound));
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
        code = 37*code + d.queryAnnotations.hashCode();
        code = 37*code + d.termAnnotations.hashCode();
        code = 37*code + d.tripleAnnotations.hashCode();
        return queryHash = code;
    }
}
