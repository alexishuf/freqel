package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;
import static java.util.stream.Collectors.toSet;

public class MutableCQuery extends CQuery {
    private static final Logger logger = LoggerFactory.getLogger(MutableCQuery.class);

    public MutableCQuery(@Nonnull CQuery query) {
        super(query.d.attach(), query.prefixDict);
    }

    protected MutableCQuery(@Nonnull List<Triple> list) {
        super(new CQueryData(list), null);
    }

    public MutableCQuery(int capacity) {
        super(new CQueryData(new ArrayList<>(capacity)), null);
    }

    public MutableCQuery() {
        super(new CQueryData(new ArrayList<>()), null);
    }

    public static @CheckReturnValue @Nonnull MutableCQuery
    from(@Nonnull Collection<Triple> c) {
        if (c instanceof CQuery)
            return new MutableCQuery((CQuery)c);
        if (c instanceof IndexedSet || c instanceof ImmutableList)
            c = new ArrayList<>(c); // do not use a immutable list as storage
        return new MutableCQuery(c instanceof List ? (List<Triple>)c
                                                            : new ArrayList<>(c));
    }

    public static @CheckReturnValue @Nonnull MutableCQuery from(@Nonnull Triple... triples) {
        return from(Arrays.asList(triples));
    }

    @VisibleForTesting
    @Nonnull CQueryCache createNewCache() {
        return new CQueryCache(d);
    }

    /* --- --- --- Modifier & annotations mutability --- --- --- */

    public @Nullable PrefixDict setPrefixDict(@Nullable PrefixDict prefixDict) {
        PrefixDict old = this.prefixDict;
        this.prefixDict = prefixDict;
        return old;
    }

    /**
     * Returns a modifiable {@link ModifiersSet}.
     */
    public @Nonnull ModifiersSet mutateModifiers() {
        makeExclusive();
        return d.modifiers;
    }

    /**
     * Remove all projections variables not in {@link CQueryCache#allVarNames()}.
     * @return true iff a change was done.
     */
    public @CanIgnoreReturnValue boolean sanitizeProjection() {
        return sanitizeProjection(false);
    }

    /**
     * Remove all projections variables not in {@link CQueryCache#tripleVarNames()}.
     * @return true iff a change was done.
     */
    public @CanIgnoreReturnValue boolean sanitizeProjectionStrict() {
        return sanitizeProjection(true);
    }

    /**
     * Remove all filters for which no variable appears in any triple.
     *
     * @return The set of removed filters, which may be empty
     */
    public @CanIgnoreReturnValue @Nonnull Set<SPARQLFilter> sanitizeFilters() {
        return sanitizeFilters(false);
    }

    /**
     * Remove all filters for which at least one variable is not present in any triple.
     *
     * This removes all filters which contain variables that would act as input variables
     * (since such variables cannot receive values from a SPARQL engine). Queries with such
     * filters cannot be evaluated by SPARQL endpoints (no bindings for such variables) but may
     * be valid arguments for a bind join that binds such variables.
     *
     * After this call, <code>attr().tripleVarNames().equals(attr().allVarNames())</code>>.
     *
     * This method also removes any filter that would be removed by the non-strict overload.
     *
     * @return the set of removed filters
     */
    public @CanIgnoreReturnValue @Nonnull Set<SPARQLFilter> sanitizeFiltersStrict() {
        return sanitizeFilters(true);
    }

    /**
     * Add a query-level annotation.
     * @return true if the annotation was not already present, else otherwise.
     */
    public @CanIgnoreReturnValue boolean annotate(@Nonnull QueryAnnotation queryAnnotation) {
        makeExclusive();
        boolean change = d.queryAnnotations.add(queryAnnotation);
        if (change)
            d.cache.invalidateQueryAnnotations();
        return change;
    }

    /**
     * Annotate a triple, if the triple is in the query.
     *
     * @return true if the triple was not previously annotated with the given annotation.
     * @throws IllegalArgumentException if the triple is not in this query
     */
    public @CanIgnoreReturnValue
    boolean annotate(@Nonnull Triple triple,
                     @Nonnull TripleAnnotation annotation) throws IllegalArgumentException {
        return annotate(triple, Collections.singleton(annotation));
    }

    /**
     * Annotate a triple with all given annotations, if not already present.
     * @return true if this query was modified, false otherwise.
     * @throws IllegalArgumentException If the triple is not part of this query.
     */
    public @CanIgnoreReturnValue
    boolean annotate(@Nonnull Triple triple,
                     @Nonnull Collection<TripleAnnotation> coll) throws IllegalArgumentException {
        checkArgument(d.cache.getSet().contains(triple), "Triple not in query");
        makeExclusive();
        boolean change = d.tripleAnnotations.putAll(triple, coll);
        if (change) {
            for (TripleAnnotation a : coll) d.cache.notifyTripleAnnotationChange(a.getClass());
        }
        return change;
    }

    /**
     * Copy all annotations in other if the annotated triple exists in this.
     *
     * @return true if this query was modified, false otherwise
     */
    public boolean copyTripleAnnotations(@Nonnull CQuery other) {
        makeExclusive();
        boolean change = false;
        for (Triple triple : d.list)
            change |= d.tripleAnnotations.putAll(triple, other.d.tripleAnnotations.get(triple));
        if (change)
            d.cache.invalidateTermAnnotations();
        return change;
    }


    /**
     * Annotate the given term with the given annotation.
     *
     * @return true if the term was not already annotated with the given annotation.
     * @throws IllegalArgumentException if term is not part of any triple or filter.
     */
    public @CanIgnoreReturnValue
    boolean annotate(@Nonnull Term term,
                     @Nonnull TermAnnotation annotation) throws IllegalArgumentException {
        return annotate(term, Collections.singleton(annotation));
    }

    /**
     * Annotate a term with a {@link Collection} of annotations.
     * @return true if the query was modified, false otherwise
     * @throws IllegalArgumentException if term is not part of any triple or filter
     */
    public @CanIgnoreReturnValue boolean
    annotate(@Nonnull Term term,
             @Nonnull Collection<TermAnnotation> collection) throws IllegalArgumentException {
        checkArgument(d.cache.allTerms().contains(term), "Term not in any triple or filter");
        makeExclusive();
        boolean change = d.termAnnotations.putAll(term, collection);
        if (change) {
            for (TermAnnotation annotation : collection)
                d.cache.notifyTermAnnotationChange(annotation.getClass());
        }
        return change;
    }

    /**
     * Copy all annotations in other if the annotated term exists in this.
     *
     * @return true if this query was modified, false otherwise
     */
    public boolean copyTermAnnotations(@Nonnull CQuery other) {
        makeExclusive();
        boolean change = false;
        for (Term term : d.cache.allTerms())
            change |= d.termAnnotations.putAll(term, other.d.termAnnotations.get(term));
        if (change)
            d.cache.invalidateTermAnnotations();
        return change;
    }

    /**
     * Remove a query-level annotation, if present
     * @return true iff the annotation was removed (it was present)
     */
    public @CanIgnoreReturnValue boolean deannotate(@Nonnull QueryAnnotation annotation) {
        makeExclusive();
        boolean change = d.queryAnnotations.remove(annotation);
        if (change)
            d.cache.invalidateQueryAnnotations();
        return change;
    }

    /**
     * Remove the given annotation from triple, if the triple is part of this query and is
     * annotated with it.
     * @return true if removed, false otherwise
     */
    public @CanIgnoreReturnValue boolean deannotate(@Nonnull Triple triple, @Nonnull TripleAnnotation annotation) {
        assert d.cache.getSet().contains(triple) : "Triple not in query, likely an error";
        makeExclusive();
        boolean change = d.tripleAnnotations.remove(triple, annotation);
        if (change)
            d.cache.notifyTripleAnnotationChange(annotation.getClass());
        return change;
    }

    /**
     * Remove all annotations on triple, if triple is part of this query.
     * @return true if any annotation was removed
     */
    public @CanIgnoreReturnValue boolean deannotate(@Nonnull Triple triple) {
        assert d.cache.getSet().contains(triple) : "Triple not in query, likely an error";
        makeExclusive();
        Set<TripleAnnotation> removed = d.tripleAnnotations.removeAll(triple);
        removed.forEach(a -> d.cache.notifyTripleAnnotationChange(a.getClass()));
        return !removed.isEmpty();
    }

    /**
     * Remove all annotations of the given triple that match the given predicate
     * @return true if the query was changed, false otherwise
     */
    public @CanIgnoreReturnValue
    boolean deannotateTripleIf(@Nonnull Triple triple,
                               @Nonnull Predicate<TripleAnnotation> predicate) {
        List<TripleAnnotation> victims = null;
        for (TripleAnnotation ann : d.tripleAnnotations.get(triple)) {
            if (predicate.test(ann)) {
                if (victims == null)
                    victims = new ArrayList<>();
                victims.add(ann);
            }
        }
        if (victims != null) {
            makeExclusive();
            d.tripleAnnotations.get(triple).removeAll(victims);
            for (TripleAnnotation ann : victims)
                d.cache.notifyTripleAnnotationChange(ann.getClass());
        }
        return victims != null;
    }

    /**
     * Remove all triple annotations that satisfy the predicate
     * @return true if any annotation was removed, false otherwise
     */
    public @CanIgnoreReturnValue
    boolean deannotateTripleIf(@Nonnull Predicate<TripleAnnotation> predicate) {
        return deannotateTripleIf((t, a) -> predicate.test(a));
    }

    /**
     * Remove all triple annotations that satisfy the predicate
     * @return true if any annotation was removed, false otherwise
     */
    public @CanIgnoreReturnValue
    boolean deannotateTripleIf(@Nonnull BiPredicate<Triple, TripleAnnotation> predicate) {
        makeExclusive();
        boolean change = d.tripleAnnotations.entries()
                          .removeIf(e -> predicate.test(e.getKey(), e.getValue()));
        if (change)
            d.cache.invalidateTripleAnnotations();
        return change;
    }

    /**
     * Remove the given annotation from the given term if term is in query and is annotated with it.
     * @return true if the annotation was removed, false otherwise
     */
    public @CanIgnoreReturnValue boolean deannotate(@Nonnull Term term, @Nonnull TermAnnotation annotation) {
        assert d.cache.allTerms().contains(term) : "Term not in query, likely an error";
        makeExclusive();
        boolean change = d.termAnnotations.remove(term, annotation);
        if (change)
            d.cache.notifyTermAnnotationChange(annotation.getClass());
        return change;
    }

    /**
     * Remove all annotations from term, if it is part of this query.
     * @return true if any annotation was removed, false otherwise
     */
    public @CanIgnoreReturnValue boolean deannotate(@Nonnull Term term) {
        assert d.cache.allTerms().contains(term) : "Term not in query, likely an error";
        makeExclusive();
        Set<TermAnnotation> removed = d.termAnnotations.removeAll(term);
        removed.forEach(a -> d.cache.notifyTermAnnotationChange(a.getClass()));
        return !removed.isEmpty();
    }

    /**
     * Remove all annotations of given term that match the given predicate
     * @return true if any annotation was removed
     */
    public @CanIgnoreReturnValue
    boolean deannotateTermIf(@Nonnull Term term, @Nonnull Predicate<TermAnnotation> predicate) {
        List<TermAnnotation> victims = null;
        for (TermAnnotation ann : d.termAnnotations.get(term)) {
            if (predicate.test(ann)) {
                if (victims == null) victims = new ArrayList<>();
                victims.add(ann);
            }
        }
        if (victims != null) {
            makeExclusive();
            d.termAnnotations.get(term).removeAll(victims);
            for (TermAnnotation annotation : victims)
                d.cache.notifyTermAnnotationChange(annotation.getClass());
        }
        return victims != null;
    }

    /**
     * Remove all term annotations that match the given predicate
     * @return true if any annotation was removed
     */
    public @CanIgnoreReturnValue boolean deannotateTermIf(@Nonnull Predicate<TermAnnotation> predicate) {
        return deannotateTermIf((t, a) -> predicate.test(a));
    }

    /**
     * Remove all term annotations that match the given predicate
     * @return true if any annotation was removed
     */
    public @CanIgnoreReturnValue boolean deannotateTermIf(@Nonnull BiPredicate<Term, TermAnnotation> predicate) {
        makeExclusive();
        boolean change = d.termAnnotations.entries()
                          .removeIf(e -> predicate.test(e.getKey(), e.getValue()));
        if (change)
            d.cache.invalidateTermAnnotations();
        return change;
    }

    /* --- --- --- Query-level mutations --- --- --- */

    /**
     * Checks if a merge with the given query would not throw {@link UnsafeMergeException}.
     *
     * If this returns true, a call to {@link MutableCQuery#mergeWith(CQuery)} may still return
     * false due to no changes being made.
     *
     * @param other tentative argument to {@link MutableCQuery#mergeWith(CQuery)}.
     * @return true if {@link MutableCQuery#mergeWith(CQuery)}  would not throw
     *         {@link UnsafeMergeException}, false otherwise
     */
    public boolean canMergeWith(@Nonnull CQuery other) {
        return d.modifiers.canMergeWith(other.getModifiers());
    }

    /**
     * Adds all triples, annotations and modifiers of the given query into this.
     *
     * If a triple from other is already present at this, then annotations on the triple or
     * its term are still subject to being added into this query.
     *
     * If any of the two queries has a projection modifier, a new merged modifier will be used.
     * Its required flag will be escalated (if any is required, the merged will also be required)
     * and the projection contents will be the union of {@link CQueryCache#publicVarNames()}
     * on both queries before any change is done. In the special case where the other query
     * adds no triple, annotation or modifier and has no Projection modifier,
     * the {@link Projection} of this query, if exists, will not be modified.
     *
     *
     * @throws UnsafeMergeException if the merge is unsafe (see
     *                              {@link Capability#isMergeUnsafe()}).
     * @return true if this query was changed (new triples, annotations or modifiers).
     */
    @CanIgnoreReturnValue
    public boolean mergeWith(@Nonnull CQuery other) throws UnsafeMergeException {
        d.modifiers.checkCanMergeWith(other.getModifiers());
        boolean change = false, triplesChange = false;
        IndexedSet<String> oldPublicVars = attr().publicVarNames();
        IndexedSet<Triple> oldSet = attr().getSet();
        makeExclusive();
        for (Triple triple : other) {
            if (!oldSet.contains(triple)) {
                change = triplesChange = true;
                d.list.add(triple);
            }
            for (TripleAnnotation a : other.getTripleAnnotations(triple)) {
                if (d.tripleAnnotations.put(triple, a)) {
                    d.cache.notifyTripleAnnotationChange(a.getClass());
                    change = true;
                }
            }
        }
        if (triplesChange)
            d.cache.invalidateTriples();
        for (Term term : other.attr().tripleTerms()) {
            for (TermAnnotation a : other.getTermAnnotations(term)) {
                if (d.termAnnotations.put(term, a)) {
                    d.cache.notifyTermAnnotationChange(a.getClass());
                    change = true;
                }
            }
        }

        try {
            change |= d.modifiers.safeMergeWith(other.getModifiers(), oldPublicVars,
                    other.attr().publicVarNames());
        } catch (UnsafeMergeException e) {
            assert false : "Earlier check failed to throw, query is already mutated!";
            throw e;
        }
        return change;
    }

    /* --- --- --- Mutable Iterator/ListIterator --- --- --- */

    protected class It extends CQuery.It {
        private @Nullable Triple last = null;

        public It(Iterator<Triple> it) {
            super(it);
        }

        @Override public Triple next() {
            return last = super.next();
        }

        @Override public void remove() {
            checkState(last != null, "Cannot remove(), no previous next() call!");
            MutableCQuery.this.remove(last);
            last = null;
        }
    }

    protected class ListIt implements ListIterator<Triple> {
        private int cursor, lastReturned = -1;

        public ListIt(int cursor) {
            assert cursor >= 0 && cursor <= size();
            this.cursor = cursor;
        }

        @Override public boolean hasNext() {
            assert cursor >= 0;
            return cursor < size();
        }
        @Override public boolean hasPrevious() {
            assert cursor >= 0;
            return cursor > 0 && !isEmpty();
        }

        @Override public Triple next() {
            if (!hasNext()) throw new NoSuchElementException();
            return get(lastReturned = cursor++);
        }
        @Override public Triple previous() {
            if (!hasPrevious()) throw new NoSuchElementException();
            return get(lastReturned = --cursor);
        }

        @Override public int nextIndex() { return cursor; }
        @Override public int previousIndex() { return cursor - 1; }

        @Override public void remove() {
            checkState(lastReturned >= -1, "Cannot remove(): add() called");
            checkState(lastReturned >= 0, "Cannot remove(): No previous next*()/previous*() call");
            MutableCQuery.this.remove(lastReturned);
            cursor = lastReturned;
        }

        @Override public void set(Triple triple) {
            checkArgument(triple != null, "Null Triples not allowed");
            checkState(lastReturned >= -1, "Cannot set(): remove()/add() called");
            checkState(lastReturned >= 0, "Cannot set(): no previous next*()/previous*() call");
            MutableCQuery.this.set(lastReturned, triple); //may have no effect
        }

        @Override public void add(Triple triple) {
            checkArgument(triple != null, "Null Triples not allowed");
            checkNotNull(triple);
            assert !isEmpty() || cursor == 0;
            assert cursor >= 0 && cursor <= size();
            lastReturned = -2;
            if (MutableCQuery.this.contains(triple))
                return; //add() would be a no-op
            MutableCQuery.this.add(cursor, triple);
            assert get(cursor) == triple;
            ++cursor; //next() will be unaffected and previous() will return triple
            assert hasPrevious();
        }
    }

    /* --- --- --- Triple mutability --- --- --- */

    @Override
    @Nonnull public Iterator<Triple> iterator() {
        return new It(d.list.iterator());
    }

    @Override
    @Nonnull public ListIterator<Triple> listIterator() {
        return new ListIt(0);
    }

    @Override
    @Nonnull public ListIterator<Triple> listIterator(int index) {
        checkPositionIndex(index, size()+1);
        return new ListIt(index);
    }

    @SuppressWarnings("Contract") // violates the List contract since the list is distinct
    @Override
    public final boolean add(Triple triple) {
        if (d.cache.getSet().contains(triple))
            return false;
        makeExclusive();
        d.list.add(triple);
        d.cache.invalidateTriples();
        return true;
    }

    /**
     * Add triples to represent a path between a subject and object. The path is expanded into
     * a sequence of triple patterns, using surrogate variables for object-subject joins as needed.
     *
     * @return true if any new triple was effectively added, false otherwise.
     */
    public boolean add(@Nonnull Term subj, @Nonnull SimplePath path,
                       @Nonnull Term obj) {
        if (path.isEmpty())
            return false;
        makeExclusive();
        Term focus = subj;
        ArrayList<Term> terms = new ArrayList<>();
        for (Iterator<SimplePath.Segment> it = path.getSegments().iterator(); it.hasNext(); ) {
            SimplePath.Segment segment = it.next();
            Term oldFocus = focus;
            focus = it.hasNext() ? nextHidden() : obj;
            if (segment.isReverse()) {
                terms.add(focus);
                terms.add(segment.getTerm());
                terms.add(oldFocus);
            } else {
                terms.add(oldFocus);
                terms.add(segment.getTerm());
                terms.add(focus);
            }
        }
        assert (terms.size() % 3) == 0;
        boolean change = false;
        for (int i = 0; i < terms.size(); i += 3)
            change |= d.list.add(new Triple(terms.get(i), terms.get(i+1), terms.get(i+2)));
        if (change)
            d.cache.invalidateTriples();
        return change;
    }

    @Override
    public final boolean remove(Object o) {
        if (!(o instanceof Triple)) return false;
        return doRemoveAll(Collections.singleton((Triple)o), "remove", o);
    }
    @Override
    public boolean addAll(@Nonnull Collection<? extends Triple> c) {
        return addAll(size(), c);
    }
    @Override
    public boolean addAll(int index, @Nonnull Collection<? extends Triple> c) {
        IndexedSet<Triple> set = attr().getSet();
        boolean change = c.stream().anyMatch(t -> !set.contains(t));
        if (change) {
            makeExclusive();
            for (Triple triple : c) {
                if (set.contains(triple)) continue;
                d.list.add(index, triple);
                ++index; //adjust for next insertion
            }
            d.cache.invalidateTriples();
        }
        return change;
    }
    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        //noinspection unchecked
        return doRemoveAll((Collection<Triple>)c, "removeAll", c);
    }

    @Override
    public boolean removeIf(Predicate<? super Triple> filter) {
        return doRemoveAll(attr().getSet().subset(filter), "removeIf", filter);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        @SuppressWarnings("unchecked")
        IndexedSubset<Triple> victims = attr().getSet().subset((Collection<Triple>) c);
        assert victims.getBitSet().length() <= d.list.size();
        victims.getBitSet().flip(0, d.list.size());
        return doRemoveAll(victims, "retainAll", c);
    }
    @Override
    public void clear() {
        doRemoveAll(this, "clear", null);
    }
    @Override
    public @Nonnull Triple set(int index, Triple element) {
        int existingIndex = d.cache.getSet().indexOf(element);
        if (existingIndex == index)
            return element; // no change
        Triple old = d.list.get(index);
        Set<String> oldInputs = getFilterInputsIfAsserting();
        makeExclusive();
        d.list.set(index, element);
        if (existingIndex >= 0)  // remove at old position since we do not allow duplicates
            d.list.remove(existingIndex);
        // apply effects of removing old triple at index
        d.tripleAnnotations.removeAll(old);
        d.cache.invalidateTriples();
        IndexedSet<Term> allTerms = d.cache.allTerms();
        d.termAnnotations.keySet().removeIf(t -> !allTerms.contains(t));
        assert checkNewFilterInputs(oldInputs, "set", index+","+element);
        return old;
    }
    @Override
    public void add(int index, Triple element) {
        int existingIndex = d.cache.getSet().indexOf(element);
        if (existingIndex == index)
            return; //no work
        makeExclusive();
        if (existingIndex >= 0) { // swap position within the list
            d.list.remove(existingIndex);
            if (existingIndex < index) --index; //adjust to avoid index out of bounds
        }
        d.list.add(index, element);
        d.cache.invalidateTriples();
    }
    @Override
    public @Nonnull Triple remove(int index) {
        Triple triple = d.list.get(index);
        doRemoveAll(Collections.singleton(triple), "remove", index);
        return triple;
    }

    /* --- --- --- Internals --- --- ---  */

    protected synchronized void makeExclusive() {
        this.d = d.toExclusive();
    }

    protected @Nonnull Var nextHidden() {
        return new StdVar("_riefederator_"+d.nextHiddenId());
    }

    private boolean sanitizeProjection(boolean strict) {
        Projection p = d.modifiers.projection();
        if (p == null) return false;
        IndexedSet<String> allowed = strict ? attr().tripleVarNames() : attr().allVarNames();
        Set<String> current = p.getVarNames();
        IndexedSubset<String> fixed = allowed.subset(current);
        if (fixed.size() == current.size())
            return false; // no change
        boolean change = d.modifiers.add(new Projection(fixed));
        assert change;
        return true;
    }

    private @Nonnull Set<SPARQLFilter> sanitizeFilters(boolean strict) {
        Set<SPARQLFilter> filters = d.modifiers.filters();
        if (filters.isEmpty())
            return Collections.emptySet();
        makeExclusive();
        Set<SPARQLFilter> set = Sets.newHashSetWithExpectedSize(filters.size());
        IndexedSet<String> tripleVars = attr().tripleVarNames();
        for (Iterator<Modifier> it = d.modifiers.iterator(); it.hasNext(); ){
            Modifier modifier = it.next();
            if (modifier instanceof SPARQLFilter) {
                SPARQLFilter filter = (SPARQLFilter) modifier;
                boolean remove = strict ? !tripleVars.containsAll(filter.getVarTermNames())
                                        : !tripleVars.containsAny(filter.getVarTermNames());
                if (remove) {
                    it.remove();
                    set.add(filter);
                }
            }
        }
        d.cache.notifyModifierChange(SPARQLFilter.class);
        return set;
    }

    private boolean doRemoveAll(@Nonnull Collection<Triple> coll, String method, Object arg) {
        IndexedSet<Triple> set = attr().getSet();
        IndexedSubset<Triple> subset = set.subset(coll);
        if (subset.isEmpty())
            return false;
        Set<String> oldInputs = getFilterInputsIfAsserting();
        makeExclusive();
        ArrayList<Triple> updated = new ArrayList<>(d.list.size());
        BitSet bs = subset.getBitSet();
        for (int i = bs.nextClearBit(0); i < d.list.size(); i = bs.nextClearBit(i+1))
            updated.add(d.list.get(i));
        coll.forEach(d.tripleAnnotations::removeAll);
        d.list = updated;
        d.cache.invalidateTriples();
        IndexedSet<Term> allTerms = d.cache.allTerms();
        d.termAnnotations.keySet().removeIf(t -> !allTerms.contains(t));
        d.cache.invalidateTermAnnotations();
        assert checkNewFilterInputs(oldInputs, method, arg);
        return true;
    }

    private @Nullable Set<String> getFilterInputsIfAsserting() {
        if (MutableCQuery.class.desiredAssertionStatus()) {
            IndexedSet<String> tripleVars = d.cache.tripleVarNames();
            return d.modifiers.filters().stream()
                    .flatMap(f -> f.getVarTermNames().stream())
                    .filter(n -> !tripleVars.contains(n)).collect(toSet());
        }
        return null;
    }

    private boolean checkNewFilterInputs(@Nullable Set<String> oldFilterInputs,
                                         @Nonnull String method, @Nullable Object arg) {
        if (!MutableCQuery.class.desiredAssertionStatus())
            return true; // only run if assertions are enabled
        Set<String> newInputs = getFilterInputsIfAsserting();
        assert oldFilterInputs != null && newInputs != null;
        if (oldFilterInputs.isEmpty() && !newInputs.isEmpty()) {
            logger.warn("{}({}) introduced filter input variables to query that " +
                        "had none: {}. Query: {}", method, arg, newInputs, this);
            return true;
        } else if (!oldFilterInputs.containsAll(newInputs)) {
            logger.debug("{}({}) changed set of filter input vars to {} from {} " +
                         "in query {}", method, arg, newInputs, oldFilterInputs, this);
        }
        return true;
    }

}
