package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.LazyCartesianResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.intersect;
import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

public class ModifiersSet extends AbstractSet<Modifier> {
    private static final Logger logger = LoggerFactory.getLogger(ModifiersSet.class);
    private static final class Data {
        private final @Nonnull List<Set<Modifier>> sets;
        private final @Nonnull Set<SPARQLFilter> filtersView;
        private int size;

        public Data(@Nullable Data other) {
            Capability[] capabilities = Capability.values();
            sets = new ArrayList<>(capabilities.length);
            for (Capability cap : Capability.values()) {
                Set<Modifier> otherSet = other == null ? null : other.sets.get(cap.ordinal());
                if (cap.isUniqueModifier())
                    sets.add(otherSet == null ? emptySet() : otherSet);
                else
                    sets.add(otherSet == null ? new HashSet<>() : new HashSet<>(otherSet));
            }
            Set<?> filters = sets.get(Capability.SPARQL_FILTER.ordinal());
            //noinspection unchecked
            filtersView = unmodifiableSet((Set<SPARQLFilter>)filters);
            size = other == null ? 0 : other.size;
        }
    }
    private final @Nonnull Data d;
    private final @Nonnull Set<Listener> listeners = new HashSet<>();
    private boolean locked = false;

    public static final @Nonnull ModifiersSet EMPTY = new ModifiersSet().getLockedView();

    protected ModifiersSet(@Nonnull Data d) {
        this.d = d;
        locked = true;
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

    public static class Listener {
        public void added(@Nonnull Modifier modifier) {}
        public void removed(@Nonnull Modifier modifier) {}
    }

    public void addListener(@Nonnull Listener listener) {
        if (locked)
            logger.warn("addListener({}) on locked view: will never notify", listener);
        listeners.add(listener);
    }
    public void removeListener(@Nonnull Listener listener) {
        if (locked)
            logger.warn("removeListener({}) on locked view: will never notify", listener);
        listeners.remove(listener);
    }

    protected void added(@Nonnull Modifier modifier) {
        for (Listener listener : listeners) listener.added(modifier);
    }

    protected void removed(@Nonnull Modifier modifier) {
        for (Listener listener : listeners) listener.removed(modifier);
    }

    @Override
    public int size() {
        return d.size;
    }

    @Override
    public boolean isEmpty() {
        return d.size == 0;
    }

    private class It implements Iterator<Modifier> {

        private int currentRow = 0;
        private Iterator<Modifier> it = null;
        private Modifier current = null, last = null;

        private boolean advance() {
            int endRow = d.sets.size();
            while (currentRow != endRow && current == null) {
                if (it == null)
                    it = d.sets.get(currentRow).iterator();
                if (it.hasNext()) {
                    current = it.next();
                } else {
                    it = null;
                    ++currentRow;
                }
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
            assert it != null;
            if (Capability.values()[currentRow].isUniqueModifier()) {
                d.sets.set(currentRow, Collections.emptySet());
                ++currentRow;
                it = null;
            } else {
                it.remove();
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
        Modifier modifier = (Modifier) o;
        return d.sets.get(modifier.getCapability().ordinal()).contains(modifier);
    }

    @Override
    public boolean add(@Nullable Modifier modifier) {
        checkNotNull(modifier);
        checkState(!locked, "ModifiersSet is unmodifiable");
        Capability capability = modifier.getCapability();
        int ordinal = capability.ordinal();
        if (capability.isUniqueModifier()) {
            Iterator<Modifier> it = d.sets.get(ordinal).iterator();
            boolean empty = !it.hasNext();
            if (empty || !it.next().equals(modifier)) {
                d.sets.set(ordinal, Collections.singleton(modifier));
                if (empty)
                    ++d.size;
                added(modifier);
                return true;
            }
            return false;
        }
        boolean change = d.sets.get(ordinal).add(modifier);
        if (change) {
            ++d.size;
            added(modifier);
        }
        return change;
    }

    @Override
    public boolean remove(Object o) {
        checkState(!locked, "ModifiersSet is unmodifiable");
        if (!(o instanceof Modifier)) return false;
        Modifier m = (Modifier) o;
        Capability capability = m.getCapability();

        boolean change = false;
        if (capability.isUniqueModifier()) {
            Iterator<Modifier> it = d.sets.get(capability.ordinal()).iterator();
            if (it.hasNext() && it.next().equals(m)) {
                d.sets.set(capability.ordinal(), Collections.emptySet());
                change = true;
            }
        } else {
            change = d.sets.get(capability.ordinal()).remove(m);
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
                change |= add(new Projection(union(myVars, theirs.getVarNames())));
            } else {
                change = mergeNonProjection(modifier, change);
            }
        }
        Projection p = projection();
        if (!gotProjection && p != null && !collectionFallbackProjection.isEmpty()) {
            Set<String> vars = union(p.getVarNames(), collectionFallbackProjection);
            change |= add(new Projection(vars));
        }
        return change;
    }

    private @Nullable UnsafeMergeException getUnsafeMergeException(@Nonnull Collection<Modifier> collection) {
        List<Modifier> list = null;
        for (Modifier modifier : collection) {
            Capability capability = modifier.getCapability();
            if (capability.isMergeUnsafe()) {
                Set<Modifier> curr = d.sets.get(capability.ordinal());
                if (capability.hasParameter() ? !curr.contains(modifier) : curr.isEmpty()) {
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
        return new ModifiersSet(d);
    }

    private @Nonnull <T extends Modifier> Set<T> get(@Nonnull Capability capability,
                                                    @Nonnull Class<T> modifierClass) {
        //noinspection unchecked
        Set<T> set = (Set<T>)d.sets.get(capability.ordinal());
        return locked ? unmodifiableSet(set) : set;
    }

    private @Nullable <T extends Modifier> T getSingleton(@Nonnull Capability cap,
                                                         @Nonnull Class<T> modifierClass) {
        assert cap.isUniqueModifier() : "this should not be called for non-unique capabilities";
        Iterator<Modifier> it = d.sets.get(cap.ordinal()).iterator();
        //noinspection unchecked
        return it.hasNext() ? (T) it.next() : null;
    }

    public @Nullable Projection projection() {
        return getSingleton(Capability.PROJECTION, Projection.class);
    }
    public @Nullable Ask ask() {
        return getSingleton(Capability.ASK, Ask.class);
    }
    public @Nullable Distinct distinct() {
        return getSingleton(Capability.DISTINCT, Distinct.class);
    }
    public @Nullable Limit limit() {
        return getSingleton(Capability.LIMIT, Limit.class);
    }
    public @Nullable Optional optional() {
        return getSingleton(Capability.OPTIONAL, Optional.class);
    }
    public @Nonnull Set<SPARQLFilter> filters() {
        return d.filtersView;
    }
    public @Nullable ValuesModifier valueModifier() {
        return getSingleton(Capability.VALUES, ValuesModifier.class);
    }
}
