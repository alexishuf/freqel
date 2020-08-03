package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.description.MatchAnnotation;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.impl.CQueryData;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.util.ImmutableIndexedSubset;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;

public class CQueryCache {
    private @Nonnull final CQueryData d;
    private List<Triple> unmodifiableList;
    private Set<Modifier> unmodifiableModifiers;
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

    private final AtomicInteger indexTriplesState = new AtomicInteger(0);
    private List<ImmutableIndexedSubset<Triple>> t2triple,  s2triple, p2triple, o2triple;
    private SetMultimap<Term, Atom> t2atom;
    private IndexedSet<Triple> matchedTriples;
    private Set<SPARQLFilter> filters;
    private Set<Projection> projection;

    private Boolean joinConnected, ask, distinct;
    private int limit = -1;
    private int queryHash = 0;

    public CQueryCache(@Nonnull CQueryData data) {
        this.d = data;
    }

    public CQueryCache(@Nonnull CQueryData data, @Nonnull CQueryCache other) {
        this.d = data;
        set = other.set;
        unmodifiableList = other.unmodifiableList;
        unmodifiableModifiers = other.unmodifiableModifiers;
        unmodifiableQueryAnnotations = other.unmodifiableQueryAnnotations;
        tripleTerms = other.tripleTerms;
        allTerms = other.allTerms;
        tripleVars = other.tripleVars;
        allVars = other.allVars;
        tripleVarNames = other.tripleVarNames;
        allVarNames = other.allVarNames;
        publicTripleVarNames = other.publicTripleVarNames;
        publicVarNames = other.publicVarNames;
        indexTriplesState.set(other.indexTriplesState.get());
        t2triple = other.t2triple;
        s2triple = other.s2triple;
        p2triple = other.p2triple;
        o2triple = other.o2triple;
        t2atom = other.t2atom;
        matchedTriples = other.matchedTriples;
        filters = other.filters;
        projection = other.projection;
        joinConnected = other.joinConnected;
        ask = other.ask;
        distinct = other.distinct;
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
        publicTripleVarNames = publicVarNames = null;
        indexTriplesState.set(0);
        t2triple = null;
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
        if (AtomAnnotation.class.isAssignableFrom(annClass))
            t2atom = null;
        queryHash = 0;
    }

    void invalidateTermAnnotations() {
        t2atom = null;
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
            filters = null;
            allVars = null;
            allVarNames = null;
            allTerms = null;
        } else if (Limit.class.isAssignableFrom(modClass)) {
            limit = -1;
        } else if (Ask.class.isAssignableFrom(modClass)) {
            ask = null;
            publicVarNames = null;
            publicTripleVarNames = null;
        } else if (Distinct.class.isAssignableFrom(modClass)) {
            distinct = null;
        } else if (Projection.class.isAssignableFrom(modClass)) {
            publicVarNames = null;
            publicTripleVarNames = null;
            projection = null;
        }
        unmodifiableModifiers = null;
        queryHash = 0;
    }

    public @Nonnull IndexedSet<Triple> getSet() {
        if (set == null)
            set = IndexedSet.from(d.list);
        return set;
    }

    void setSet(@Nonnull IndexedSet<Triple> set) {
        assert this.set == null || this.set.equals(set);
        assert this.set != null || IndexedSet.from(d.list).equals(set);
        this.set = set;
    }

    public @Nonnull List<Triple> unmodifiableList() {
        if (unmodifiableList == null)
            unmodifiableList = Collections.unmodifiableList(d.list);
        return unmodifiableList;
    }

    public @Nonnull Set<Modifier> unmodifiableModifiers() {
        if (unmodifiableModifiers == null)
            unmodifiableModifiers = unmodifiableSet(d.modifiers);
        return unmodifiableModifiers;
    }

    public @Nonnull Set<QueryAnnotation> unmodifiableQueryAnnotations() {
        if (unmodifiableQueryAnnotations == null)
            unmodifiableQueryAnnotations = unmodifiableSet(d.queryAnnotations);
        return unmodifiableQueryAnnotations;
    }

    public @Nonnull IndexedSet<Term> tripleTerms() {
        if (tripleTerms == null) {
            List<Term> list = new ArrayList<>();
            HashMap<Term, Integer> map = new HashMap<>(d.list.size()*2);
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
            tripleVars = IndexedSet.fromDistinct(tripleTerms().stream()
                                   .filter(Term::isVar).map(Term::asVar)
                                   .collect(toList()));
        }
        return tripleVars;
    }

    public @Nonnull IndexedSet<Term> allTerms() {
        if (allTerms != null)
            return allTerms;
        Set<SPARQLFilter> filters = filters();
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
        Set<SPARQLFilter> filters = filters();
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
            tripleVarNames = IndexedSet.fromDistinct(tripleVars().stream()
                                       .map(Var::getName).collect(toList()));
        }
        return tripleVarNames;
    }

    public @Nonnull IndexedSet<String> allVarNames() {
        if (allVarNames == null) {
            allVarNames = IndexedSet.fromDistinct(allVars().stream()
                    .map(Var::getName).collect(toList()));
        }
        return allVarNames;
    }

    public @Nonnull IndexedSet<String> publicVarNames() {
        if (publicVarNames != null)
            return publicVarNames;
        if (isAsk())
            return publicVarNames = IndexedSet.empty();
        Projection projection = projection();
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

    public @Nullable Projection projection() {
        if (this.projection == null)
            this.projection = singleton(ModifierUtils.getFirst(Projection.class, d.modifiers));
        return projection.iterator().next();
    }

    public @Nonnull Set<SPARQLFilter> filters() {
        if (filters != null)
            return filters;
        Set<SPARQLFilter> set = new HashSet<>();
        for (Modifier modifier : d.modifiers) {
            if (modifier instanceof SPARQLFilter)
                set.add((SPARQLFilter) modifier);
        }
        return filters = set;
    }


    public @Nonnull IndexedSet<Triple> matchedTriples() {
        if (matchedTriples != null)
            return matchedTriples;
        Map<Triple, Integer> map = new HashMap<>();
        List<Triple> list = new ArrayList<>();
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
        int termCount = terms.size();
        List<BitSet> sets = new ArrayList<>(termCount), sSets = new ArrayList<>(termCount);
        List<BitSet> pSets = new ArrayList<>(termCount), oSets = new ArrayList<>(termCount);
        for (int i = 0; i < termCount; i++) {
            sets.add(new BitSet());
            sSets.add(new BitSet());
            pSets.add(new BitSet());
            oSets.add(new BitSet());
        }

        for (int i = 0, size = d.list.size(); i < size; i++) {
            Triple triple = d.list.get(i);
            int sIdx = terms.indexOf(triple.getSubject()), oIdx = terms.indexOf(triple.getObject());
            int pIdx = terms.indexOf(triple.getPredicate());
            sets.get(sIdx).set(i);
            sets.get(pIdx).set(i);
            sets.get(oIdx).set(i);
            sSets.get(sIdx).set(i);
            pSets.get(pIdx).set(i);
            oSets.get(oIdx).set(i);
        }

        t2triple = toImmutableIndexedSubset(triples, sets);
        s2triple = toImmutableIndexedSubset(triples, sSets);
        p2triple = toImmutableIndexedSubset(triples, pSets);
        o2triple = toImmutableIndexedSubset(triples, oSets);
        indexTriplesState.set(2);
    }

    private static @Nonnull <T> List<ImmutableIndexedSubset<T>>
    toImmutableIndexedSubset(@Nonnull IndexedSet<T> parent, @Nonnull List<BitSet> sets) {
        assert sets.stream().noneMatch(Objects::isNull);
        List<ImmutableIndexedSubset<T>> list = new ArrayList<>();
        for (BitSet set : sets)
            list.add(new ImmutableIndexedSubset<>(parent, set));
        return list;
    }

    public @Nonnull ImmutableIndexedSubset<Triple> triplesWithTerm(@Nonnull Term term) {
        indexTriples();
        int idx = tripleTerms().indexOf(term);
        return idx < 0 ? getSet().immutableEmptySubset() : t2triple.get(idx);
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
        return ask = d.modifiers.stream().anyMatch(Ask.class::isInstance)
                || (!d.list.isEmpty() && d.list.stream().allMatch(Triple::isBound));
    }

    public boolean isDistinct() {
        if (distinct == null)
            distinct = ModifierUtils.getFirst(Distinct.class, d.modifiers) != null;
        return distinct;
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
