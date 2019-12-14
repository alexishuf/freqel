package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.Triple.Position;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.riefederator.query.JoinType.Position.OBJ;
import static br.ufsc.lapesd.riefederator.query.JoinType.Position.SUBJ;
import static com.google.common.collect.ImmutableList.builderWithExpectedSize;

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
    /** An empty {@link CQuery} instance. */
    public static final @Nonnull CQuery EMPTY = from(Collections.emptyList());

    private final @Nonnull ImmutableList<Triple> list;

    /* ~~~ cache attributes ~~~ */

    @SuppressWarnings("Immutable")
    private @LazyInit @Nonnull SoftReference<Multimap<Term, Integer>> t2triple, s2triple, o2triple;
    private @LazyInit int hash = 0;
    private @LazyInit @Nullable Boolean ask = null;

    /* ~~~ constructor & factories ~~~ */

    protected CQuery(@Nonnull ImmutableList<Triple> query) {
        this.list = query;
        t2triple = new SoftReference<>(null);
        s2triple = new SoftReference<>(null);
        o2triple = new SoftReference<>(null);
    }

    public static @Contract("_ -> new") @Nonnull CQuery from(@Nonnull ImmutableList<Triple> query) {
        return new CQuery(query);
    }
    public static @Nonnull CQuery from(@Nonnull List<Triple> query) {
        return query instanceof CQuery ? (CQuery) query : new CQuery(ImmutableList.copyOf(query));
    }
    public static @Contract("_ -> new") @Nonnull CQuery from(@Nonnull Triple triple) {
        return new CQuery(ImmutableList.of(triple));
    }

    /* ~~~ CQuery methods ~~~ */

    /** Gets the underlying immutable triple {@link List} of this {@link CQuery}. */
    public @Nonnull ImmutableList<Triple> getList() { return list; }

    /** A {@link CQuery} is a ASK-type query iff all its triples are bound
     * (i.e., no triple has a {@link Var} term). */
    public boolean isAsk() {
        if (ask == null)
            ask = !list.isEmpty() && list.stream().allMatch(Triple::isBound);
        return ask;
    }

    /** All terms are bound, either because <code>isAsk()</code> or because it is empty. */
    public boolean allBound() {
        return isAsk() || list.isEmpty();
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
        return new CQuery(builder.build());
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

    @Override
    public boolean equals(Object o) {
        return (o instanceof CQuery) && list.equals(((CQuery) o).list);
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = list.hashCode();
        return hash;
    }

    @Override
    public @Nonnull String toString() {
        return toString(StdPrefixDict.DEFAULT);
    }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        if (list.isEmpty()) return "{}";
        StringBuilder b = new StringBuilder(list.size()*16);
        b.append("{\n");
        for (Triple t : list) {
            b.append("  ").append(t.getSubject().toString(dict))
                    .append(t.getPredicate().toString(dict))
                    .append(t.getObject().toString(dict)).append(" .\n");
        }
        return b.append("}").toString();
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
                System.out.printf("i=%d :: %s\n", i, triple);
                map.put(triple.getSubject(), i);
                map.put(triple.getPredicate(), i);
                map.put(triple.getObject(), i);
            }
            System.out.println(t2triple);
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
