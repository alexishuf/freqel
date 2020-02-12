package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.description.MatchAnnotation;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.Triple.Position;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.JoinType.Position.OBJ;
import static br.ufsc.lapesd.riefederator.query.JoinType.Position.SUBJ;
import static com.google.common.collect.ImmutableList.builderWithExpectedSize;
import static java.util.Collections.emptySet;

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

    private final @Nonnull ImmutableList<Triple> list;
    private final @Nonnull ImmutableList<Modifier> modifiers;
    @SuppressWarnings("Immutable") // PrefixDict is not immutable
    private final @Nullable PrefixDict prefixDict;

    private final @Nullable ImmutableSetMultimap<Term, TermAnnotation> termAnnotations;
    private final @Nullable ImmutableSetMultimap<Triple, TripleAnnotation> tripleAnnotations;

    /* ~~~ cache attributes ~~~ */

    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Multimap<Term, Integer>> t2triple, s2triple, o2triple;
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Set<Var>> varsCache
            = new SoftReference<>(null);
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<ImmutableSet<Triple>> set
            = new SoftReference<>(null);
    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Set<Triple>> matchedTriples
            = new SoftReference<>(null);
    private @LazyInit int hash = 0;
    private @LazyInit @Nullable Boolean ask = null;

    /* ~~~ constructor, builder & factories ~~~ */

    public CQuery(@Nonnull ImmutableList<Triple> query, @Nonnull ImmutableList<Modifier> modifiers,
                  @Nullable PrefixDict prefixDict,
                  @Nullable ImmutableSetMultimap<Term, TermAnnotation> termAnn,
                  @Nullable ImmutableSetMultimap<Triple, TripleAnnotation> tripleAnn) {
        this.list = query;
        this.modifiers = modifiers;
        this.prefixDict = prefixDict;
        this.termAnnotations = termAnn != null && termAnn.isEmpty() ? null : termAnn;
        this.tripleAnnotations = tripleAnn != null && tripleAnn.isEmpty() ? null : tripleAnn;
        if (CQuery.class.desiredAssertionStatus()) {
            Set<Term> terms = streamTerms(Term.class).collect(Collectors.toSet());
            boolean[] fail = {false};
            forEachTermAnnotation((t, a) -> {
                if ((fail[0] |= !terms.contains(t)))
                    logger.error("Foreign term {} has annotation {} in {}!", t, a, CQuery.this);
            });
            ImmutableSet<Triple> triples = getSet();
            forEachTripleAnnotation((t, a) -> {
                if ((fail[0] |= !triples.contains(t)))
                    logger.error("Foreign Triple {} has annotation {} in {}!", t, a, CQuery.this);
            });
            Preconditions.checkArgument(!fail[0], "Foreign annotations (see the logger output)");
        }
        t2triple = new SoftReference<>(null);
        s2triple = new SoftReference<>(null);
        o2triple = new SoftReference<>(null);
    }

    public CQuery(@Nonnull ImmutableList<Triple> query,
                  @Nonnull ImmutableList<Modifier> modifiers, @Nullable PrefixDict prefixDict) {
        this(query, modifiers, prefixDict, null, null);
    }

    public CQuery(@Nonnull ImmutableList<Triple> query,
                  @Nonnull ImmutableList<Modifier> modifiers) {
        this(query, modifiers, null);
    }

    public static class WithBuilder {
        protected  @Nullable ImmutableList<Triple> list;
        private Projection.Builder projection = null;
        private boolean distinct = false, ask = false;
        private boolean distinctRequired = false, askRequired = false;
        private @Nullable PrefixDict prefixDict = null;
        private @Nullable ImmutableSetMultimap.Builder<Term, TermAnnotation> termAnnBuilder;
        private @Nullable ImmutableSetMultimap.Builder<Triple, TripleAnnotation> tripleAnnBuilder;

        protected WithBuilder() {
            this.list = null;
        }
        public WithBuilder(@Nonnull ImmutableList<Triple> list) {
            this.list = list;
        }

        @CanIgnoreReturnValue
        @Contract("_ -> this")
        public @Nonnull WithBuilder project(String... names) {
            if (projection == null)
                projection = Projection.builder();
            for (String name : names) projection.add(name);
            return this;
        }

        @Contract("_ -> this")
        @CanIgnoreReturnValue
        public @Nonnull WithBuilder project(Var... vars) {
            if (projection == null)
                projection = Projection.builder();
            for (Var var : vars) projection.add(var.getName());
            return this;
        }
        @CanIgnoreReturnValue
        public @Contract("-> this") @Nonnull WithBuilder requireProjection() {
            if (projection == null)
                projection = Projection.builder();
            projection.required();
            return this;
        }
        @CanIgnoreReturnValue
        public @Contract("-> this") @Nonnull WithBuilder adviseProjection() {
            if (projection == null)
                projection = Projection.builder();
            projection.advised();
            return this;
        }

        @CanIgnoreReturnValue
        public @Contract("_ -> this") @Nonnull WithBuilder distinct(boolean required) {
            this.distinct = true;
            this.distinctRequired = required;
            return this;
        }
        @CanIgnoreReturnValue
        public @Contract("-> this") @Nonnull WithBuilder distinct() { return distinct(true); }
        @CanIgnoreReturnValue
        public @Contract("-> this") @Nonnull WithBuilder nonDistinct() {
            this.distinct = this.distinctRequired = false;
            return this;
        }

        @CanIgnoreReturnValue
        public @Contract("_ -> this") @Nonnull WithBuilder ask(boolean required) {
            this.ask = true;
            this.askRequired = required;
            return this;
        }
        @CanIgnoreReturnValue
        public @Contract("-> this") @Nonnull WithBuilder ask() { return ask(true); }

        @CanIgnoreReturnValue
        public @Contract("_ -> this") @Nonnull WithBuilder prefixDict(@Nonnull PrefixDict dict) {
            prefixDict = dict;
            return this;
        }

        @CanIgnoreReturnValue
        public @Contract("_, _ -> this") @Nonnull
        WithBuilder annotate(@Nonnull Term term, @Nonnull TermAnnotation annotation) {
            if (termAnnBuilder == null) termAnnBuilder = ImmutableSetMultimap.builder();
            termAnnBuilder.put(term, annotation);
            return this;
        }

        @CanIgnoreReturnValue
        public @Contract("_, _ -> this") @Nonnull
        WithBuilder annotate(@Nonnull Triple triple, @Nonnull TripleAnnotation annotation) {
            if (tripleAnnBuilder == null) tripleAnnBuilder = ImmutableSetMultimap.builder();
            tripleAnnBuilder.put(triple, annotation);
            return this;
        }

        @CanIgnoreReturnValue
        public @Contract("_ -> this") @Nonnull
        WithBuilder annotateAllTerms(Multimap<Term, TermAnnotation> multimap) {
            if (termAnnBuilder == null) termAnnBuilder = ImmutableSetMultimap.builder();
            termAnnBuilder.putAll(multimap);
            return this;
        }

        @CanIgnoreReturnValue
        public @Contract("_ -> this") @Nonnull
        WithBuilder annotateAllTriples(Multimap<Triple, TripleAnnotation> multimap) {
            if (tripleAnnBuilder == null) tripleAnnBuilder = ImmutableSetMultimap.builder();
            tripleAnnBuilder.putAll(multimap);
            return this;
        }

        @CheckReturnValue
        public @Nonnull CQuery build() {
            @SuppressWarnings("UnstableApiUsage")
            ImmutableList.Builder<Modifier> b = builderWithExpectedSize(4);
            if (projection != null)
                b.add(projection.build());
            if (distinct)
                b.add(distinctRequired ? Distinct.REQUIRED : Distinct.ADVISED);
            if (ask)
                b.add(askRequired ? Ask.REQUIRED : Ask.ADVISED);
            ImmutableSetMultimap<Term, TermAnnotation> termAnn =
                    termAnnBuilder == null ? null : termAnnBuilder.build();
            ImmutableSetMultimap<Triple, TripleAnnotation> tripleAnn =
                    tripleAnnBuilder == null ? null : tripleAnnBuilder.build();
            assert list != null;
            return new CQuery(list, b.build(), prefixDict, termAnn, tripleAnn);
        }
    }

    public static class Builder extends WithBuilder {
        private List<Triple> mutableList;
        private int size = 0;

        public Builder() {
            mutableList = new ArrayList<>();
        }
        public Builder(int sizeHint) {
            mutableList = new ArrayList<>(sizeHint);
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public @Contract("_ -> this") @Nonnull Builder add(@Nonnull Triple... triples) {
            size += triples.length;
            mutableList.addAll(Arrays.asList(triples));
            return this;
        }
        public @Contract("_ -> this") @Nonnull Builder addAll(@Nonnull Collection<Triple> triples) {
            size += triples.size();
            mutableList.addAll(triples);
            if (triples instanceof CQuery) {
                ((CQuery) triples).forEachTermAnnotation(  this::annotate);
                ((CQuery) triples).forEachTripleAnnotation(this::annotate);
            }
            return this;
        }

        public @Nonnull List<Triple> getList() {
            return mutableList;
        }

        @Override
        public @Contract("_ -> this") @Nonnull Builder project(String... names) {
            super.project(names);
            return this;
        }

        @Override
        public @Contract("_ -> this") @Nonnull Builder project(Var... vars) {
            super.project(vars);
            return this;
        }

        @Override
        public @Contract("-> this") @Nonnull Builder requireProjection() {
            super.requireProjection();
            return this;
        }

        @Override
        public @Contract("-> this") @Nonnull Builder adviseProjection() {
            super.adviseProjection();
            return this;
        }

        @Override
        public @Contract("_ -> this") @Nonnull Builder distinct(boolean required) {
            super.distinct(required);
            return this;
        }

        @Override
        public @Contract("-> this") @Nonnull Builder distinct() {
            super.distinct();
            return this;
        }

        @Override
        public @Contract("-> this") @Nonnull Builder nonDistinct() {
            super.nonDistinct();
            return this;
        }

        @Override
        public @Contract("_ -> this") @Nonnull Builder ask(boolean required) {
            super.ask(required);
            return this;
        }

        @Override
        public @Contract("-> this") @Nonnull Builder ask() {
            super.ask();
            return this;
        }

        @Override
        public @Contract("_ -> this") @Nonnull Builder prefixDict(@Nonnull PrefixDict dict) {
            super.prefixDict(dict);
            return this;
        }

        @Override
        public @Contract("_, _ -> this") @Nonnull
        Builder annotate(@Nonnull Term term, @Nonnull TermAnnotation annotation) {
            super.annotate(term, annotation);
            return this;
        }

        @Override
        public @Contract("_, _ -> this") @Nonnull
        Builder annotate(@Nonnull Triple triple, @Nonnull TripleAnnotation annotation) {
            super.annotate(triple, annotation);
            return this;
        }

        @Override @Contract("_ -> this")
        public @Nonnull WithBuilder annotateAllTerms(Multimap<Term, TermAnnotation> multimap) {
            super.annotateAllTerms(multimap);
            return this;
        }

        @Override @Contract("_ -> this")
        public @Nonnull WithBuilder annotateAllTriples(Multimap<Triple, TripleAnnotation> multimap) {
            super.annotateAllTriples(multimap);
            return this;
        }

        @Override
        public @Nonnull CQuery build() {
            list = ImmutableList.copyOf(mutableList);
            return super.build();
        }
    }

    @CheckReturnValue
    public static @Contract("_ -> new") @Nonnull WithBuilder with(@Nonnull ImmutableList<Triple> query) {
        return new WithBuilder(query);
    }

    @CheckReturnValue
    public static @Nonnull WithBuilder with(@Nonnull Collection<Triple> query) {
        if (query instanceof CQuery)
            return new WithBuilder(((CQuery)query).getList());
        if (query instanceof ImmutableList)
            return new WithBuilder(((ImmutableList<Triple>)query));
        return new WithBuilder(ImmutableList.copyOf(query));
    }

    @CheckReturnValue
    public static @Contract("_ -> new") @Nonnull WithBuilder with(@Nonnull Triple... triples) {
        return new WithBuilder(ImmutableList.copyOf(triples));
    }

    @CheckReturnValue
    public static @Nonnull CQuery from(@Nonnull Collection<Triple> query) {
        return query instanceof CQuery ? (CQuery)query : with(query).build();
    }

    @CheckReturnValue
    public static @Contract("_ -> new") @Nonnull CQuery from(@Nonnull Triple... triples) {
        return new CQuery(ImmutableList.copyOf(triples), ImmutableList.of());
    }

    public static @Contract("-> new") @Nonnull Builder builder() {
        return new Builder();
    }
    public static @Contract("_ -> new") @Nonnull Builder builder(int expectedTriples) {
        return new Builder(expectedTriples);
    }

    public @Contract("_ -> new") @Nonnull CQuery withPrefixDict(@Nullable PrefixDict dict) {
        CQuery copy = new CQuery(list, modifiers, dict);
        copy.hash = hash;
        copy.ask = ask;
        copy.varsCache = varsCache;
        copy.set = set;
        copy.matchedTriples = matchedTriples;
        copy.t2triple = t2triple;
        copy.s2triple = s2triple;
        copy.o2triple = o2triple;
        return copy;
    }

    /* ~~~ CQuery methods ~~~ */

    /** Gets the underlying immutable triple {@link List} of this {@link CQuery}. */
    public @Nonnull ImmutableList<Triple> getList() { return list; }

    /** Gets all {@link Triple}s in this query in an {@link ImmutableSet}. */
    public @Nonnull ImmutableSet<Triple> getSet() {
        ImmutableSet<Triple> strong = set.get();
        if (strong == null) {
            set = new SoftReference<>(strong = ImmutableSet.copyOf(getList()));
        }
        return strong;
    }

    /** Gets the modifiers of this query. */
    public @Nonnull ImmutableList<Modifier> getModifiers() { return modifiers; }

    /** Indicates if there is any triple annotation. */
    public boolean hasTripleAnnotations() { return tripleAnnotations != null; }

    /** Indicates whether there is some term annotation in this query. */
    public boolean hasTermAnnotations() { return termAnnotations != null;}

    @SuppressWarnings("unchecked")
    public boolean hasAnnotation(@Nonnull Class<?> cls) {
        boolean has = false;
        if (TermAnnotation.class.isAssignableFrom(cls))
            has = forEachTermAnnotation((Class<? extends TermAnnotation>)cls, (t, a) -> {});
        if (!has && TripleAnnotation.class.isAssignableFrom(cls))
            has = forEachTripleAnnotation((Class<? extends TripleAnnotation>)cls, (t, a) -> {});
        return has;
    }

    /**
     * Gets the term annotations for the given term
     * @param term Term to look for, need not occur in this query
     * @return Possibly-empty {@link Collection} of {@link TermAnnotation}s.
     */
    public @Nonnull Collection<TermAnnotation> getTermAnnotations(@Nonnull Term term) {
        return termAnnotations == null ? emptySet() : termAnnotations.get(term);
    }

    /**
     * Gets the {@link TripleAnnotation} set on the given {@link Triple}.
     * @param triple {@link Triple} to look for, need not occur in this query.
     * @return Possibly-empty {@link Collection} of {@link TripleAnnotation}s.
     */
    public @Nonnull Collection<TripleAnnotation> getTripleAnnotations(@Nonnull Triple triple) {
        return tripleAnnotations == null ? emptySet() : tripleAnnotations.get(triple);
    }

    public @Nonnull Set<Triple> getMatchedTriples() {
        Set<Triple> strong = matchedTriples.get();
        if (strong == null) {
            strong = new HashSet<>(size());
            for (Triple triple : getList()) {
                boolean has = false;
                for (TripleAnnotation ann : getTripleAnnotations(triple)) {
                    if ((has = ann instanceof MatchAnnotation))
                        strong.add(((MatchAnnotation) ann).getMatched());
                }
                if (!has)
                    strong.add(triple);
            }
            matchedTriples = new SoftReference<>(strong);
        }
        return strong;
    }

    public void forEachTermAnnotation(@Nonnull BiConsumer<Term, TermAnnotation> consumer) {
        if (termAnnotations != null)
            termAnnotations.forEach(consumer);
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
        if (tripleAnnotations != null)
            tripleAnnotations.forEach(consumer);
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


    /** A {@link CQuery} is a ASK-type query iff all its triples are bound
     * (i.e., no triple has a {@link Var} term). */
    public boolean isAsk() {
        if (ask == null)
            ask = !list.isEmpty() && list.stream().allMatch(Triple::isBound);
        return ask;
    }

    public @Nullable PrefixDict getPrefixDict() {
        return prefixDict;
    }

    public @Nonnull PrefixDict getPrefixDict(@Nonnull PrefixDict fallback) {
        return prefixDict == null ? fallback : prefixDict;
    }

    /** All terms are bound, either because <code>isAsk()</code> or because it is empty. */
    public boolean allBound() {
        return isAsk() || list.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public <T extends Term> Stream<T> streamTerms(@Nonnull Class<T> cls) {
        if (cls == Var.class) {
            Set<Var> strong = varsCache.get();
            if (strong == null) {
                strong = list.stream().flatMap(Triple::stream)
                        .filter(t -> t instanceof Var).map(t -> (Var) t)
                        .collect(Collectors.toSet());
            }
            return (Stream<T>)strong.stream();
        }
        return list.stream().flatMap(Triple::stream)
                .filter(t -> cls.isAssignableFrom(t.getClass())).map(t -> (T)t).distinct();
    }

    /**
     * Starting from joinTerm in triple get all other triples in query that are join-reachable.
     *
     * @param policy The closure policy to apply. The policy has no effect over
     *               <code>joinTerm</code>'s position within <code>triple</code>.
     * @param joinTerm Join variable for first hop
     * @param triple Triple from which to start exploring. It will not be included in the result.
     *               If null, it will be ignored.
     * @return A new {@link CQuery} with a subset of {@link Triple}s from query.
     */
    @Contract(value = "_, _, _ -> new", pure = true)
    public @Nonnull CQuery joinClosure(@Nonnull JoinType policy, @Nonnull Term joinTerm,
                                       @Nullable Triple triple) {
        JoinClosureWalker walker = new JoinClosureWalker(policy);
        if (triple != null) {
            int tripleIdx = list.indexOf(triple);
            Preconditions.checkArgument(tripleIdx >= 0, "triple must be in query");
            walker.ban(tripleIdx);
            Preconditions.checkArgument(triple.contains(joinTerm), "joinTerm must be in triple");
        }

        walker.visit(joinTerm);
        return walker.build();
    }

    @Contract(value = "_, _ -> new", pure = true)
    public @Nonnull CQuery joinClosure(@Nonnull JoinType policy, @Nonnull Term joinTerm) {
        return joinClosure(policy, joinTerm, null);
    }

    /**
     * Starting from the given triples, get the join-closure as a new query.
     *
     * @param triples A collection of triples from which to start exploring
     * @param include whether to include the input triples in the closure. Default is false.
     * @param policy Join policy to consider during exploration
     * @return The closure as a new {@link CQuery}
     */
    public @Nonnull CQuery joinClosure(@Nonnull Collection<Triple> triples, boolean include,
                                       @Nonnull JoinType policy) {
        if (triples.isEmpty()) return CQuery.EMPTY;
        JoinClosureWalker walker = new JoinClosureWalker(policy);
        List<Integer> indices = triples.stream().map(list::indexOf).collect(Collectors.toList());
        Preconditions.checkArgument(indices.stream().allMatch(i -> i >= 0),
                "There are triples which are not part of this CQuery");
        if (!include)
            indices.forEach(walker::ban);
        for (Triple triple : triples)
            policy.forEachSourceAt(triple, (t, pos) -> walker.visit(t));
        if (include)
            walker.visited.addAll(indices); //ensure all triples are included
        return walker.build();
    }

    /** Equivalent to <code>joinClosure(triples, include, JoinType.VARS)</code> */
    public @Nonnull CQuery joinClosure(@Nonnull Collection<Triple> triples,
                                       boolean include) {
        return joinClosure(triples, include, JoinType.VARS);
    }

    /** Equivalent to <code>joinClosure(triples, false, policy)</code> */
    public @Nonnull CQuery joinClosure(@Nonnull Collection<Triple> triples,
                                       @Nonnull JoinType policy) {
        return joinClosure(triples, false, policy);
    }

    /** Equivalent to <code>joinClosure(triples, false, JoinType.VARS)</code> */
    public @Nonnull CQuery joinClosure(@Nonnull Collection<Triple> triples) {
        return joinClosure(triples, JoinType.VARS);
    }

    /**
     * Gets a sub-query with all triples where the given term appears in one of the positions.
     *
     * @param term The {@link Term} to look for
     * @param positions {@link Position}s in which the term may occur
     * @return A possibly empty {@link CQuery} with the subset of triples
     */
    public @Nonnull CQuery containing(@Nonnull Term term, Collection<Position> positions) {
        ArrayList<Integer> indices = new ArrayList<>(2*size());
        if (positions.size() == 3 && positions.containsAll(Position.VALUES_LIST)) {
            indices.addAll(getTerm2Triple().get(term));
        } else {
            for (Position p : positions) {
                switch (p) {
                    case SUBJ:
                        indices.addAll(getSubj2Triple().get(term));
                        break;
                    case OBJ:
                        indices.addAll(getObj2Triple().get(term));
                        break;
                    case PRED:
                        for (Integer i : getTerm2Triple().get(term)) {
                            if (list.get(i).getPredicate().equals(term))
                                indices.add(i);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unexpected Position " + p);
                }
            }
        }
        Collections.sort(indices);
        //noinspection UnstableApiUsage
        ImmutableList.Builder<Triple> builder = builderWithExpectedSize(indices.size());
        assert indices.stream().noneMatch(i -> i < 0) : "An index cannot be negative!";
        int last = -1;
        for (int i : indices) {
            if (i != last)
                builder.add(list.get(last = i));
        }
        return new CQuery(builder.build(), getModifiers());
    }

    /** Equivalent to <code>containing(term, asList(positions))</code>. */
    public @Nonnull CQuery containing(@Nonnull Term term, Position... positions) {
        return containing(term, Arrays.asList(positions));
    }

    /* ~~~ List<> delegating methods ~~~ */

    @Override public int size() { return list.size(); }
    @Override public boolean isEmpty() { return list.isEmpty(); }
    @Override public boolean contains(Object o) { return list.contains(o); }
    @Override public @Nonnull Iterator<Triple> iterator() { return list.iterator(); }
    @Override public @Nonnull Object[] toArray() { return list.toArray(); }
    @Override public @Nonnull <T> T[] toArray(@Nonnull T[] a) {
        //noinspection unchecked
        return (T[]) toArray();
    }
    @Override @DoNotCall public final boolean add(Triple triple) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public final boolean remove(Object o) { throw new UnsupportedOperationException(); }
    @Override public boolean containsAll(@Nonnull Collection<?> c) { return list.containsAll(c);}
    @Override @DoNotCall public final boolean addAll(@Nonnull Collection<? extends Triple> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public final boolean addAll(int index, @NotNull Collection<? extends Triple> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public final boolean removeAll(@NotNull Collection<?> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public final boolean retainAll(@NotNull Collection<?> c) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public final void clear() {throw new UnsupportedOperationException();}
    @Override public Triple get(int index) { return list.get(index); }
    @Override @DoNotCall public final Triple set(int index, Triple element) { throw new UnsupportedOperationException(); }
    @Override @DoNotCall public final void add(int index, Triple element) { throw new UnsupportedOperationException();}
    @Override @DoNotCall public final Triple remove(int index) { throw new UnsupportedOperationException(); }
    @Override public int indexOf(Object o) { return list.indexOf(o); }
    @Override public int lastIndexOf(Object o) {return list.lastIndexOf(o); }
    @Override public @Nonnull ListIterator<Triple> listIterator() { return list.listIterator();}
    @Override public @Nonnull ListIterator<Triple> listIterator(int index) { return list.listIterator(index); }
    @Override public @Nonnull List<Triple> subList(int fromIndex, int toIndex) { return list.subList(fromIndex, toIndex); }

    /* ~~~ Object-ish methods ~~~ */

    /**
     * Version of equals() that does not require the triples to be in the same order
     */
    public boolean unorderedEquals(@Nullable Collection<? extends Triple> o) {
        boolean triplesEq = o != null && getSet().containsAll(o) && getSet().size() == o.size();
        if (!(o instanceof CQuery))
            return triplesEq;
        return triplesEq
                && modifiers.equals(((CQuery) o).modifiers)
                && Objects.equals(termAnnotations, ((CQuery) o).termAnnotations)
                && Objects.equals(tripleAnnotations, ((CQuery) o).tripleAnnotations);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (!(o instanceof CQuery))
            return list.equals(o); // fallback to list comparison when comparing with a list
        return list.equals(((CQuery) o).list)
                && modifiers.equals(((CQuery) o).modifiers)
                && Objects.equals(termAnnotations, ((CQuery) o).termAnnotations)
                && Objects.equals(tripleAnnotations, ((CQuery) o).tripleAnnotations);
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = Objects.hash(list, modifiers, termAnnotations, tripleAnnotations);
        return hash;
    }

    @Override
    public @Nonnull String toString() {
        return toString(getPrefixDict(StdPrefixDict.DEFAULT));
    }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        if (list.isEmpty()) return "{}";
        StringBuilder b = new StringBuilder(list.size()*16);
        b.append('{');
        if (list.size() > 1)
            b.append('\n');
        for (Triple t : list) {
            b.append("  ").append(t.getSubject().toString(dict)).append(' ')
                    .append(t.getPredicate().toString(dict)).append(' ')
                    .append(t.getObject().toString(dict)).append(" .\n");
        }
        if (list.size() == 1)
            b.setLength(b.length()-1);
        return b.append('}').toString();
    }

    /* ~~~ private methods & classes ~~~ */

    private class JoinClosureWalker {
        private final @Nonnull Set<Integer> visited = Sets.newHashSetWithExpectedSize(list.size());
        private final @Nonnull Set<Integer> banned = new HashSet<>();
        private final @Nonnull JoinType policy;
        private final @Nonnull Multimap<Term, Integer> index;
        private final boolean skipCheck;
        private @Nonnull ArrayDeque<Integer> stack = new ArrayDeque<>(list.size()*2);

        public JoinClosureWalker(@Nonnull JoinType policy) {
            this.policy = policy;
            skipCheck = policy.getTo() == OBJ || policy.getTo() == SUBJ;
            if      (policy.getTo() ==  OBJ) index =  getObj2Triple();
            else if (policy.getTo() == SUBJ) index = getSubj2Triple();
            else                             index = getTerm2Triple();
        }

        public boolean ban(int tripleIdx) {
            boolean novel = banned.add(tripleIdx);
            if (novel) visited.add(tripleIdx);
            return novel;
        }

        public @Nonnull CQuery build() {
            //noinspection UnstableApiUsage
            ImmutableList.Builder<Triple> builder = builderWithExpectedSize(visited.size());
            visited.stream().filter(i -> !banned.contains(i)).sorted()
                   .map(list::get).forEach(builder::add);
            return CQuery.from(builder.build());
        }

        @Contract("_ -> this")
        public @Nonnull JoinClosureWalker visit(Term joinTerm) {
            for (Integer idx : index.get(joinTerm)) {
                if ((skipCheck || policy.allowDestination(joinTerm, list.get(idx)))
                        && !visited.contains(idx))
                    stack.push(idx);
            }
            while (!stack.isEmpty()) {
                int idx = stack.pop();
                if (visited.add(idx)) {
                    Triple source = list.get(idx);
                    if (skipCheck) {
                        policy.forEachSourceAt(source, (t, pos) -> stack.addAll(index.get(t)));
                    } else {
                        policy.forEachSourceAt(source, (t, pos) -> {
                            for (int tgtIdx : index.get(t)) {
                                Triple tgt = list.get(tgtIdx);
                                if (!visited.contains(tgtIdx) && policy.allowDestination(t, tgt))
                                    stack.push(tgtIdx);
                            }
                        });
                    }
                }
            }
            return this;
        }
    }

    private @Nonnull Multimap<Term, Integer> getTerm2Triple() {
        Multimap<Term, Integer> map = this.t2triple.get();
        if (map == null) {
            int count = list.size() * 2;
            map = MultimapBuilder.hashKeys(Math.max(count, 16))
                                 .hashSetValues(Math.min(count, 8)).build();
            for (int i = 0; i < list.size(); i++) {
                Triple triple = list.get(i);
                map.put(triple.getSubject(), i);
                map.put(triple.getPredicate(), i);
                map.put(triple.getObject(), i);
            }
            t2triple = new SoftReference<>(map);
        }
        return map;
    }

    private @Nonnull Multimap<Term, Integer> getSubj2Triple() {
        Multimap<Term, Integer> map = this.s2triple.get();
        if (map == null) {
            map = MultimapBuilder.hashKeys(list.size()).hashSetValues(4).build();
            for (int i = 0; i < list.size(); i++) map.put(list.get(i).getSubject(), i);
            s2triple = new SoftReference<>(map);
        }
        return map;
    }

    private @Nonnull Multimap<Term, Integer> getObj2Triple() {
        Multimap<Term, Integer> map = this.o2triple.get();
        if (map == null) {
            map = MultimapBuilder.hashKeys(list.size()).hashSetValues(4).build();
            for (int i = 0; i < list.size(); i++) map.put(list.get(i).getObject(), i);
            o2triple = new SoftReference<>(map);
        }
        return map;
    }
}
