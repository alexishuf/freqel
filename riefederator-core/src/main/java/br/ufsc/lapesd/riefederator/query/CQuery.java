package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.Triple.Position;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.impl.CQueryData;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

/**
 * A {@link CQuery} is essentially a list of {@link Triple} instances which MAY contain variables.
 *
 * This class contains utility methods and cache attributes to avoid repeated computation of
 * data that can be derived from the {@link List} of {@link Triple}s. Non-trivial caches
 * use {@link SoftReference}'s to avoid cluttering the heap.
 *
 * {@link CQuery} instances are {@link Immutable} and, despite the caching, all methods
 * are {@link ThreadSafe}.
 */
@ThreadSafe
@Immutable
public class CQuery implements  List<Triple> {
    public static final @Nonnull Logger logger = LoggerFactory.getLogger(CQuery.class);

    /** An empty {@link CQuery} instance. */
    public static final @Nonnull CQuery EMPTY = from(Collections.emptyList());

    @SuppressWarnings("Immutable") // PrefixDict is not immutable
    protected @Nonnull CQueryData d;
    @SuppressWarnings("Immutable") // PrefixDict is not immutable
    protected @Nullable PrefixDict prefixDict;

    /* ~~~ constructor, builder & factories ~~~ */

    protected CQuery(@Nonnull CQueryData d, @Nullable PrefixDict prefixDict) {
        this.d = d.attach();
        this.prefixDict = prefixDict;

        if (CQuery.class.desiredAssertionStatus()) {
            checkArgument(d.cache.getSet().size() == d.list.size(), "Non-distinct triple list");
            Set<Term> terms = concat(
                    streamTerms(Term.class),
                    d.modifiers.stream().filter(SPARQLFilter.class::isInstance)
                                      .flatMap(m -> ((SPARQLFilter)m).getTerms().stream())
            ).collect(toSet());
            boolean[] fail = {false};
            forEachTermAnnotation((t, a) -> {
                if ((fail[0] |= !terms.contains(t)))
                    logger.error("Foreign term {} has annotation {} in {}!", t, a, CQuery.this);
            });
            IndexedSet<Triple> triples = d.cache.getSet();
            forEachTripleAnnotation((t, a) -> {
                if ((fail[0] |= !triples.contains(t)))
                    logger.error("Foreign Triple {} has annotation {} in {}!", t, a, CQuery.this);
            });
            checkArgument(!fail[0], "Foreign annotations (see the logger output)");
        }
    }

    @CheckReturnValue
    public static @Nonnull CQuery from(@Nonnull Collection<Triple> query) {
        if (query instanceof CQuery)
            return new CQuery(((CQuery) query).d, ((CQuery) query).prefixDict);
        List<Triple> list = query instanceof List ? (List<Triple>)query : new ArrayList<>(query);
        return new CQuery(new CQueryData(list), null);
    }

    @CheckReturnValue
    public static @Nonnull CQuery from(@Nonnull Triple... triples) {
        return from(Arrays.asList(triples));
    }

    public @Nonnull CQuery withPrefixDict(@Nullable PrefixDict dict) {
        return new CQuery(d, dict);
    }

    public @Nonnull CQuery withModifiers(@Nonnull CQuery other) {
        return withModifiers(other.getModifiers());
    }
    public @Nonnull CQuery withModifiers(@Nonnull Collection<Modifier> modifiers) {
        CQueryData d2 = d.toExclusive().attach();
        d2.modifiers.addAll(modifiers);
        return new CQuery(d2, prefixDict);
    }

    public static @Nonnull CQuery merge(@Nonnull Collection<Triple> l,
                                        @Nonnull Collection<Triple> r) {
        if (l.isEmpty()) return CQuery.from(r);
        if (r.isEmpty()) return CQuery.from(l);
        MutableCQuery merged = MutableCQuery.from(l);
        if (r instanceof CQuery)
            merged.mergeWith((CQuery)r);
        else
            merged.addAll(r);
        return merged;
    }


    /* ~~~ CQuery methods ~~~ */

    public @Nonnull CQueryCache attr() {
        return d.cache;
    }

    public @Nonnull List<Triple> asList() {
        return d.cache.unmodifiableList();
    }

    /** Gets the modifiers of this query. */
    public @Nonnull Set<Modifier> getModifiers() { return d.cache.unmodifiableModifiers(); }

    /** Indicates if there is any triple annotation. */
    public boolean hasQueryAnnotations() { return !d.queryAnnotations.isEmpty(); }

    /** Indicates if there is any query annotation of the given class. */
    public boolean hasQueryAnnotations(@Nonnull Class<? extends QueryAnnotation> ann) {
        return d.queryAnnotations.stream().anyMatch(ann::isInstance);
    }

    /** Indicates if there is any triple annotation. */
    public boolean hasTripleAnnotations() { return !d.tripleAnnotations.isEmpty(); }

    /** Indicates if there is any triple with an annotation of the given class. */
    public boolean hasTripleAnnotations(@Nonnull Class<? extends TripleAnnotation> ann) {
        return d.tripleAnnotations.entries().stream().anyMatch(e -> ann.isInstance(e.getValue()));
    }

    /** Indicates whether there is some term annotation in this query. */
    public boolean hasTermAnnotations() { return !d.termAnnotations.isEmpty();}

    /** Indicates if there is any term with an annotation of the given class. */
    public boolean hasTermAnnotations(@Nonnull Class<? extends TermAnnotation> ann) {
        return d.termAnnotations.entries().stream().anyMatch(e -> ann.isInstance(e.getValue()));
    }

    public @Nonnull Set<QueryAnnotation> getQueryAnnotations() {
        return d.cache.unmodifiableQueryAnnotations();
    }

    /**
     * Gets the term annotations for the given term
     * @param term Term to look for, need not occur in this query
     * @return Possibly-empty {@link Set} of {@link TermAnnotation}s.
     */
    public @Nonnull Set<TermAnnotation> getTermAnnotations(@Nonnull Term term) {
        return unmodifiableSet(d.termAnnotations.get(term));
    }

    /**
     * Gets the {@link TripleAnnotation} set on the given {@link Triple}.
     * @param triple {@link Triple} to look for, need not occur in this query.
     * @return Possibly-empty {@link Set} of {@link TripleAnnotation}s.
     */
    public @Nonnull Set<TripleAnnotation> getTripleAnnotations(@Nonnull Triple triple) {
        return unmodifiableSet(d.tripleAnnotations.get(triple));
    }

    public void forEachTermAnnotation(@Nonnull BiConsumer<Term, TermAnnotation> consumer) {
        d.termAnnotations.forEach(consumer);
    }

    @CanIgnoreReturnValue
    public <T extends TermAnnotation>
    boolean forEachTermAnnotation(@Nonnull Class<T> cls, @Nonnull BiConsumer<Term, T> consumer) {
        boolean[] has = {false};
        forEachTermAnnotation((t, a) -> {
            if (cls.isAssignableFrom(a.getClass())) {
                has[0] = true;
                //noinspection unchecked
                consumer.accept(t, (T) a);
            }
        });
        return has[0];
    }

    public void forEachTripleAnnotation(@Nonnull BiConsumer<Triple, TripleAnnotation> consumer) {
        d.tripleAnnotations.forEach(consumer);
    }

    @CanIgnoreReturnValue
    public <T extends TripleAnnotation>
    boolean forEachTripleAnnotation(@Nonnull Class<T> cls, @Nonnull BiConsumer<Triple, T> consumer) {
        boolean[] has = {false};
        forEachTripleAnnotation((t, a) -> {
            if (cls.isAssignableFrom(a.getClass())) {
                has[0] = true;
                //noinspection unchecked
                consumer.accept(t, (T)a);
            }
        });
        return has[0];
    }


    public @Nullable PrefixDict getPrefixDict() {
        return prefixDict;
    }

    public @Nonnull PrefixDict getPrefixDict(@Nonnull PrefixDict fallback) {
        return prefixDict == null ? fallback : prefixDict;
    }

    @SuppressWarnings("unchecked")
    public <T extends Term> Stream<T> streamTerms(@Nonnull Class<T> cls) {
        if (cls == Var.class)
            return (Stream<T>) d.cache.allVars().stream();
        return d.cache.allTerms().stream().filter(t -> cls.isAssignableFrom(t.getClass()))
                                  .map(t -> (T)t);
    }

    @SuppressWarnings("unchecked")
    public <T extends Term> Stream<T> streamTripleTerms(@Nonnull Class<T> cls) {
        if (cls == Var.class)
            return (Stream<T>) d.cache.tripleVars().stream();
        return d.cache.tripleTerms().stream().filter(t -> cls.isAssignableFrom(t.getClass()))
                                              .map(t -> (T)t);
    }

    /**
     * Gets a sub-query with all triples where the given term appears in one of the positions.
     *
     * @param term The {@link Term} to look for
     * @param positions {@link Position}s in which the term may occur
     * @return A possibly empty {@link CQuery} with the subset of triples
     */
    public @Nonnull CQuery containing(@Nonnull Term term, Collection<Position> positions) {
        IndexedSubset<Triple> triples = d.cache.getSet().emptySubset();
        for (Position position : positions)
            triples.addAll(d.cache.triplesWithTermAt(term, position));

        MutableCQuery other = MutableCQuery.from(triples);
        other.addModifiers(getModifiers());
        other.sanitizeFiltersStrict();
        other.copyTripleAnnotations(this);
        other.copyTermAnnotations(this);
        other.sanitizeProjectionStrict();
        return other;
    }

    /** Equivalent to <code>containing(term, asList(positions))</code>. */
    public @Nonnull CQuery containing(@Nonnull Term term, Position... positions) {
        return containing(term, Arrays.asList(positions));
    }

    /* ~~~ Iterator/ListIterator implementations forbidding mutations ~~~ */

    protected static class It implements Iterator<Triple> {
        private final Iterator<Triple> it;
        public It(Iterator<Triple> it) {
            this.it = it;
        }
        @Override public boolean hasNext() {
            return it.hasNext();
        }
        @Override public Triple next() {
            return it.next();
        }
        @Override public void remove() {
            throw new UnsupportedOperationException("Immutable CQuery instance");
        }
    }

    protected static class ListIt implements ListIterator<Triple> {
        private final ListIterator<Triple> it;

        public ListIt(ListIterator<Triple> it) {
            this.it = it;
        }
        @Override public boolean hasNext() {
            return it.hasNext();
        }
        @Override public Triple next() {
            return it.next();
        }
        @Override public boolean hasPrevious() {
            return it.hasPrevious();
        }
        @Override public Triple previous() {
            return it.previous();
        }
        @Override public int nextIndex() {
            return it.nextIndex();
        }
        @Override public int previousIndex() {
            return it.previousIndex();
        }
        @Override public void remove() {
            throw new UnsupportedOperationException("ListIterator cannot remove(), immutable CQuery");
        }
        @Override public void set(Triple triple) {
            throw new UnsupportedOperationException("ListIterator cannot remove(), immutable CQuery");
        }
        @Override public void add(Triple triple) {
            throw new UnsupportedOperationException("ListIterator cannot remove(), immutable CQuery");
        }
    }

    /* ~~~ List<> delegating methods ~~~ */

    @Override public int size() { return d.list.size(); }
    @Override public boolean isEmpty() { return d.list.isEmpty(); }
    @Override public boolean contains(Object o) { return attr().getSet().contains(o); }
    @Override public @Nonnull Iterator<Triple> iterator() {
        return new It(d.list.iterator());
    }
    @Override public @Nonnull Object[] toArray() { return d.list.toArray(); }
    @Override public @Nonnull <T> T[] toArray(@Nonnull T[] a) {
        //noinspection unchecked
        return (T[]) toArray();
    }
    @Override @DoNotCall public boolean add(Triple triple) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public boolean remove(Object o) { throw new UnsupportedOperationException(); }
    @Override public boolean containsAll(@Nonnull Collection<?> c) { return d.list.containsAll(c);}
    @Override @DoNotCall public boolean addAll(@Nonnull Collection<? extends Triple> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public boolean addAll(int index, @NotNull Collection<? extends Triple> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public boolean removeAll(@NotNull Collection<?> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public boolean retainAll(@NotNull Collection<?> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public void clear() {throw new UnsupportedOperationException();}
    @Override public Triple get(int index) { return d.list.get(index); }
    @Override @DoNotCall public Triple set(int index, Triple element) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public void add(int index, Triple element) { throw new UnsupportedOperationException();}
    @Override @DoNotCall public Triple remove(int index) { throw new UnsupportedOperationException(); }
    @Override public int indexOf(Object o) { return d.list.indexOf(o); }
    @Override public int lastIndexOf(Object o) {return d.list.lastIndexOf(o); }
    @Override public @Nonnull ListIterator<Triple> listIterator() { return new ListIt(d.list.listIterator());}
    @Override public @Nonnull ListIterator<Triple> listIterator(int index) { return new ListIt(d.list.listIterator(index)); }
    @Override public @Nonnull List<Triple> subList(int fromIndex, int toIndex) { return d.list.subList(fromIndex, toIndex); }

    /* ~~~ Object-ish methods ~~~ */

    @Override
    public boolean equals(@Nullable Object o) {
        if (!(o instanceof CQuery))
            return d.list.equals(o); // fallback to list comparison when comparing with a list
        return d.cache.getSet().equals(((CQuery) o).d.cache.getSet())
                && d.modifiers.equals(((CQuery) o).d.modifiers)
                && Objects.equals(d.queryAnnotations, ((CQuery) o).d.queryAnnotations)
                && Objects.equals(d.termAnnotations, ((CQuery) o).d.termAnnotations)
                && Objects.equals(d.tripleAnnotations, ((CQuery) o).d.tripleAnnotations);
    }

    @Override
    public int hashCode() {
        return d.cache.queryHash();
    }

    @Override
    public @Nonnull String toString() {
        return toString(getPrefixDict(StdPrefixDict.DEFAULT));
    }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        if (d.list.isEmpty()) return "{}";
        StringBuilder b = new StringBuilder(d.list.size()*16);
        b.append('{');
        if (d.list.size() == 1 && d.modifiers.stream().noneMatch(SPARQLFilter.class::isInstance)) {
            Triple t = d.list.iterator().next();
            return b.append(" ").append(t.getSubject().toString(dict)).append(' ')
                    .append(t.getPredicate().toString(dict)).append(' ')
                    .append(t.getObject().toString(dict)).append(" . }").toString();
        }
        boolean firstTriple = true;
        for (Triple t : d.list) {
            b.append(firstTriple ? " " : "  ")
                    .append(t.getSubject().toString(dict)).append(' ')
                    .append(t.getPredicate().toString(dict)).append(' ')
                    .append(t.getObject().toString(dict)).append(" .\n");
            firstTriple = false;
        }
        for (Modifier modifier : d.modifiers) {
            if (modifier instanceof SPARQLFilter)
                b.append("  ").append(modifier.toString()).append("\n");
        }
        b.setLength(b.length()-1);
        return b.append(" }").toString();
    }
}
