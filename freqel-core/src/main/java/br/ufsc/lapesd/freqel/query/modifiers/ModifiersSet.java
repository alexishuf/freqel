package br.ufsc.lapesd.freqel.query.modifiers;

import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.impl.LazyCartesianResults;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.intersect;
import static br.ufsc.lapesd.freqel.util.CollectionUtils.union;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

public class ModifiersSet extends AbstractSet<Modifier> {
    private static final int FILTER_ORDINAL = Capability.SPARQL_FILTER.ordinal();
    protected static final class Data {
        private final @Nonnull Modifier[] uniqueModifiers;
        private Set<SPARQLFilter> filters;
        private Set<SPARQLFilter> filtersView;
        private int size;

        public Data(@Nullable Data other) {
            Capability[] capabilities = Capability.values();
            uniqueModifiers = new Modifier[capabilities.length];
            Set<SPARQLFilter> filters = null;
            for (Capability cap : capabilities) {
                if (cap.isUniqueModifier()) {
                    int ord = cap.ordinal();
                    uniqueModifiers[ord] = other == null ? null : other.uniqueModifiers[ord];
                } else if (other != null) {
                    assert cap == Capability.SPARQL_FILTER;
                    filters = new HashSet<>(other.filters);
                }
            }
            this.filters = filters == null ? new HashSet<>() : filters;
            this.filtersView = unmodifiableSet(this.filters);
            this.size = other == null ? 0 : other.size;
        }

        @Nonnull Set<SPARQLFilter> createFilters() {
            if (filters == null)
                filtersView = unmodifiableSet(filters = new HashSet<>());
            return filters;
        }
    }
    protected final @Nonnull Data d;
    protected boolean locked = false;

    public static final @Nonnull ModifiersSet EMPTY = new ModifiersSet().getLockedView();

    protected ModifiersSet(@Nonnull ModifiersSet delegate, boolean locked) {
        this.d = delegate.d;
        this.locked = locked;
    }

    public ModifiersSet() {
        d = new Data(null);
    }

    public ModifiersSet(@Nullable Collection<? extends Modifier> collection) {
        if (collection instanceof ModifiersSet) {
            d = new Data(((ModifiersSet) collection).d);
        } else {
            d = new Data(null);
            if (collection != null)
                addAll(collection);
        }
    }

    protected void added(@Nonnull Modifier modifier) { }

    protected void removed(@Nonnull Modifier modifier) { }

    @Override
    public int size() {
        return d.size;
    }

    @Override
    public boolean isEmpty() {
        return d.size == 0;
    }

    private class It implements Iterator<Modifier> {
        private int ordinal = -1;
        private Iterator<? extends Modifier> it = null;
        private Modifier current = null, last = null;

        private boolean advance() {
            current = null;
            if (ordinal != FILTER_ORDINAL)
                ++ordinal;
            while (current == null && ordinal < d.uniqueModifiers.length) {
                if (ordinal == FILTER_ORDINAL) {
                    if (this.it != null) {
                        if (this.it.hasNext())
                            current = this.it.next();
                        else
                            this.it = null;
                    } else if (d.filters != null) {
                        Iterator<SPARQLFilter> it = d.filters.iterator();
                        if (it.hasNext())
                            current = (this.it = it).next();
                    }
                } else {
                    current = d.uniqueModifiers[ordinal];
                }
                if (current == null)
                    ++ordinal;
            }
            return current != null;
        }

        @Override
        public boolean hasNext() {
            if (current != null) return true;
            else return advance();
        }

        @Override
        public Modifier next() {
            if (!hasNext()) throw new NoSuchElementException();
            last = current;
            current = null;
            return last;
        }

        @Override
        public void remove() {
            checkState(last != null, "Cannot remove(): next() not called yet");
            checkState(!locked, "ModifiersSet is unmodifiable");
            if (it != null) {
                assert ordinal == FILTER_ORDINAL;
                it.remove();
            } else {
                d.uniqueModifiers[ordinal] = null;
            }
            --d.size;
            removed(last);
        }
    }

    @Override
    public @Nonnull Iterator<Modifier> iterator() {
        return new It();
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof Modifier)) return false;
        Modifier m = (Modifier) o;
        int ord = m.getCapability().ordinal();
        return ord == FILTER_ORDINAL ? d.filters != null && d.filters.contains(m)
                                     : d.uniqueModifiers[ord] != null;
    }

    @Override
    public boolean add(@Nullable Modifier modifier) {
        if (modifier == null)
            return false; //do not add nulls
        checkState(!locked, "ModifiersSet is unmodifiable");
        int ordinal = modifier.getCapability().ordinal();
        boolean change;
        if (ordinal == FILTER_ORDINAL) {
            change = d.createFilters().add((SPARQLFilter) modifier);
            if (change)
                ++d.size;
        } else {
            assert modifier.getCapability().isUniqueModifier();
            Modifier old = d.uniqueModifiers[ordinal];
            d.uniqueModifiers[ordinal] = modifier;
            if (old == null)
                ++d.size;
            change = old == null || !old.equals(modifier);
        }
        if (change)
            added(modifier);
        return change;

    }

    @Override
    public boolean remove(@Nullable Object o) {
        if (o == null)
            return false; //no nulls are ever stored
        checkState(!locked, "ModifiersSet is unmodifiable");
        if (!(o instanceof Modifier)) return false;
        Modifier m = (Modifier) o;
        int ordinal = m.getCapability().ordinal();

        boolean change;
        if (ordinal == FILTER_ORDINAL) {
            change = d.filters != null && d.filters.remove(m);
        } else {
            assert m.getCapability().isUniqueModifier();
            Modifier old = d.uniqueModifiers[ordinal];
            change = old != null && old.equals(m);
            if (change)
                d.uniqueModifiers[ordinal] = null;
        }
        if (change) {
            --d.size;
            removed(m);
        }
        return change;
    }

    private boolean mergeWith(@Nonnull Collection<Modifier> collection,
                             @Nonnull Collection<String> fallbackProjection,
                             @Nonnull Collection<String> collectionFallbackProjection,
                             boolean safe) throws UnsafeMergeException {
        boolean change = false, gotProjection = false;
        if (safe)
            checkCanMergeWith(collection);
        for (Modifier modifier : collection) {
            // merge modifiers
            if (modifier instanceof Projection) {
                gotProjection = true;
                Projection theirs = (Projection)modifier;
                Projection mine = projection();
                Collection<String> myVars = mine == null ? fallbackProjection : mine.getVarNames();
                change |= add(Projection.of(union(myVars, theirs.getVarNames())));
            } else {
                change = mergeNonProjection(modifier, change);
            }
        }
        Projection p = projection();
        if (!gotProjection && p != null && !collectionFallbackProjection.isEmpty()) {
            Set<String> vars = union(p.getVarNames(), collectionFallbackProjection);
            change |= add(Projection.of(vars));
        }
        return change;
    }

    private @Nullable UnsafeMergeException getUnsafeMergeException(@Nonnull Collection<Modifier> collection) {
        List<Modifier> list = null;
        for (Modifier modifier : collection) {
            Capability cap = modifier.getCapability();
            if (cap.isMergeUnsafe()) {
                int ordinal = cap.ordinal();
                assert ordinal == FILTER_ORDINAL || cap.isUniqueModifier();
                //noinspection SuspiciousMethodCalls
                boolean bad = ordinal == FILTER_ORDINAL
                        ? d.filters == null || !d.filters.contains(modifier)
                        : !Objects.equals(d.uniqueModifiers[ordinal], modifier);
                if (bad) {
                    if (list == null) list = new ArrayList<>();
                    list.add(modifier);
                }
            }
        }
        return list != null ? new UnsafeMergeException(list) : null;
    }

    private boolean mergeNonProjection(@Nonnull Modifier modifier, boolean change) {
        if (modifier instanceof ValuesModifier) {
            ValuesModifier theirs = (ValuesModifier) modifier;
            ValuesModifier mine = valueModifier();
            if (mine != null) {
                Set<String> all = union(mine.getVarNames(), theirs.getVarNames());
                Set<String> shared = intersect(mine.getVarNames(), theirs.getVarNames());
                Results mergeResults, mr = mine.createResults(), tr = theirs.createResults();
                if (shared.isEmpty())
                    mergeResults = new LazyCartesianResults(asList(mr, tr), all);
                else
                    mergeResults = new InMemoryHashJoinResults(mr, tr, shared, all);
                change |= add(ValuesModifier.fromResults(mergeResults));
            } else {
                change |= add(theirs);
            }
        } else if (modifier instanceof Limit) {
            int theirs = ((Limit) modifier).getValue();
            Limit mine = limit();
            int value = Math.min(mine == null ? Integer.MAX_VALUE : mine.getValue(), theirs);
            change |= add(new Limit(value));
        } else if (modifier instanceof Optional) {
            Optional mine = optional();
            boolean explicit =  (mine != null && mine.isExplicit())
                             || ((Optional)modifier).isExplicit();
            change |= add(explicit ? Optional.EXPLICIT : Optional.IMPLICIT);
        } else {
            assert !modifier.getCapability().isUniqueModifier()
                    || !modifier.getCapability().hasParameter();
            change |= add(modifier);
        }
        return change;
    }

    /**
     * Merge all given modifiers with this set, assuming both this and the given
     * collection apply to query fragments that are being combined by conjunction or
     * by a cartesian product. When merging {@link Capability#isUniqueModifier()} modifiers,
     * the resulting modifier will be required if any of the input modifiers is required.
     *
     * The following rules are applied during the merge:
     * - filters: simply added (if not already present)
     * - projection: union of projected vars (or fallbackProjection)
     * - values: join (if there are shared variables) or cartesian product of bindings
     * - limit: minimum value among existing modifiers
     * - ask: only the required flag rules apply
     *
     * @param collection the collection of modifiers to add
     * @param fallbackProjection if this set has no projection, consider this to be the
     *                           Projection, with non-required status
     * @param collectionFallbackProjection if this set has a projection and collection does
     *                                     not contain a Projection, consider this as the
     *                                     Projection of collection with non-required status.
     *
     * @return true if this set was modified, false otherwise.
     */
    public boolean unsafeMergeWith(@Nonnull Collection<Modifier> collection,
                                   @Nonnull Collection<String> fallbackProjection,
                                   @Nonnull Collection<String> collectionFallbackProjection) {
        return mergeWith(collection, fallbackProjection, collectionFallbackProjection, false);
    }

    /**
     * Behaves as {@link ModifiersSet#unsafeMergeWith(Collection, Collection, Collection)}, but
     * throws an exception if the merge operation would potentially cause loss of solutions.
     *
     * @throws UnsafeMergeException if safe is true and an unsafe modifier not yet
     *                              present on this set would be added.
     * @return true if this set was modified, false otherwise.
     */
    public boolean safeMergeWith(@Nonnull Collection<Modifier> collection,
                                 @Nonnull Collection<String> fallbackProjection,
                                 @Nonnull Collection<String> collectionFallbackProjection) {
        return mergeWith(collection, fallbackProjection, collectionFallbackProjection, true);
    }

    /**
     * Check if a call to {@link ModifiersSet#safeMergeWith(Collection, Collection, Collection)}
     * with the given modifiers would fail with {@link UnsafeMergeException}.
     *
     * @return true if the merge would succeed, false if would throw {@link UnsafeMergeException}.
     */
    public boolean canMergeWith(@Nonnull Collection<Modifier> collection) {
        return getUnsafeMergeException(collection) == null;
    }

    /**
     * Same as {@link ModifiersSet#canMergeWith(Collection)}, but throws instead of returning false.
     *
     * @throws UnsafeMergeException if the merge is not safe
     */
    public void checkCanMergeWith(@Nonnull Collection<Modifier> collection) {
        UnsafeMergeException ex = getUnsafeMergeException(collection);
        if (ex != null)
            throw ex;
    }

    public boolean isLocked() {
        return locked;
    }

    public @Nonnull ModifiersSet getLockedView() {
        return new ModifiersSet(this, true);
    }

    public @Nullable Projection projection() {
        return (Projection) d.uniqueModifiers[Capability.PROJECTION.ordinal()];
    }
    public @Nullable Ask ask() {
        return (Ask) d.uniqueModifiers[Capability.ASK.ordinal()];
    }
    public @Nullable Distinct distinct() {
        return (Distinct) d.uniqueModifiers[Capability.DISTINCT.ordinal()];
    }
    public @Nullable Limit limit() {
        return (Limit) d.uniqueModifiers[Capability.LIMIT.ordinal()];
    }
    public @Nullable Optional optional() {
        return (Optional) d.uniqueModifiers[Capability.OPTIONAL.ordinal()];
    }
    public @Nonnull Set<SPARQLFilter> filters() {
        return d.filtersView;
    }
    public @Nullable ValuesModifier valueModifier() {
        return (ValuesModifier) d.uniqueModifiers[Capability.VALUES.ordinal()];
    }
}
