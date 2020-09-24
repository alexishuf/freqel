package br.ufsc.lapesd.riefederator.util.indexed.subset;

import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.NotInParentException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class BitSetOps {
    public static @Nullable BitSet getBitSet(@Nonnull List<?> parent, @Nonnull Collection<?> c) {
        if (c instanceof IndexSubset) {
            IndexSubset<?> ss = (IndexSubset<?>) c;
            if (ss.getParent() == parent)
                return ss.getBitSet();
        }
        return null;
    }

    public static boolean containsAll(@Nonnull BitSet left, @Nonnull BitSet right) {
        BitSet tmp = (BitSet) right.clone();
        tmp.andNot(left);
        return tmp.isEmpty();
    }

    public static @Nonnull BitSet intersect(@Nonnull List<?> parent, @Nonnull BitSet result,
                                            @Nonnull Collection<?> other) {
        BitSet bs = getBitSet(parent, other);
        if (bs == null) {
            bs = new BitSet(result.size());
            for (Object o : other) {
                int idx = parent.indexOf(o);
                if (idx >= 0)
                    bs.set(idx);
            }
        }
        result.and(bs);
        return result;
    }

    public static @Nonnull <U> BitSet intersect(@Nonnull List<?> parent, @Nonnull BitSet result,
                                                @Nonnull U value) {
        int idx = parent.indexOf(value);
        boolean had = idx >= 0 && result.get(idx);
        result.clear();
        if (had) result.set(idx);
        return result;
    }

    public static @Nonnull <T> BitSet union(@Nonnull IndexSet<? super T> parent, @Nonnull BitSet result,
                                        @Nonnull Collection<? extends T> other) {
        BitSet bs = getBitSet(parent, other);
        if (bs != null) {
            result.or(bs);
        } else {
            for (T v : other)
                setBitChecked(parent, result, v);
        }
        return result;
    }

    public static @Nonnull <T> BitSet subset(@Nonnull IndexSet<? super T> index,
                                             @Nonnull IndexSubset<? super T> subset,
                                             @Nonnull BitSet bits,
                                             @Nonnull Collection<T> collection) {
        BitSet result = new BitSet(index.size());
        for (T value : collection)
            addToSubset(index, subset, bits, result, value);
        return result;
    }

    public static @Nonnull <T> BitSet subset(@Nonnull IndexSet<? super T> index,
                                             @Nonnull IndexSubset<? super T> subset,
                                             @Nonnull BitSet bits,
                                             @Nonnull T value) {
        BitSet result = new BitSet(index.size());
        addToSubset(index, subset, bits, result, value);
        return result;
    }

    private static <T> void addToSubset(@Nonnull IndexSet<? super T> index,
                                        @Nonnull IndexSubset<? super T> subset,
                                        @Nonnull BitSet bits, @Nonnull BitSet result,
                                        @Nonnull T value) {
        int i = index.indexOf(value);
        if (i >= 0) {
            if (bits.get(i))
                result.set(i);
            else
                throw new NotInParentException(value, subset);
        } else {
            throw new NotInParentException(value, index);
        }
    }

    public static @Nonnull <T> BitSet subsetExpanding(@Nonnull IndexSet<? super T> parent,
                                                      @Nonnull Collection<T> values) {
        if (values.isEmpty())
            return new BitSet();
        BitSet bs = getBitSet(parent, values);
        if (bs != null)
            return (BitSet) bs.clone();
        BitSet result = new BitSet(parent.size() + values.size());
        for (T v : values)
            result.set(parent.safeAdd(v));
        return result;
    }

    public static @Nonnull <T> BitSet union(@Nonnull List<? extends T> parent,
                                            @Nonnull BitSet result,
                                            @Nonnull Predicate<? super T> predicate) {
        int i = 0;
        for (T o : parent) {
            if (predicate.test(o))
                result.set(i);
            ++i;
        }
        return result;
    }

    public static @Nonnull <T> BitSet union(@Nonnull IndexSet<? super T> parent,
                                            @Nonnull BitSet result, @Nonnull T value) {
        setBitChecked(parent, result, value);
        return result;
    }

    private static <T> void setBitChecked(@Nonnull IndexSet<? super T> parent,
                                          @Nonnull BitSet result, @Nonnull T value) {
        int idx = parent.indexOf(value);
        if (idx >= 0)
            result.set(idx);
        else
            throw new NotInParentException(value, parent);
    }

    public static @Nonnull BitSet complement(@Nonnull List<?> parent, @Nonnull BitSet result) {
        result.flip(0, parent.size());
        return result;
    }

    public static @Nonnull <T> BitSet symDiff(@Nonnull IndexSet<? super T> parent,
                                              @Nonnull BitSet result,
                                              @Nonnull Collection<? extends T> other) {
        BitSet bs = getBitSet(parent, other);
        if (bs != null) {
            result.xor(bs);
        } else if (result.isEmpty()) {
            for (T v : other)
                setBitChecked(parent, result, v);
        } else if (!other.isEmpty()) {
            for (T v : other) {
                int idx = parent.indexOf(v);
                if (idx < 0) throw new NotInParentException(v, parent);
                result.set(idx, !result.get(idx));
            }
        }
        return result;
    }

    public static @Nonnull BitSet subtract(@Nonnull List<?> parent, @Nonnull BitSet result,
                                           @Nonnull Collection<?> other) {
        BitSet bs = getBitSet(parent, other);
        if (bs != null) {
            result.andNot(bs);
        } else if (result.isEmpty() || other.isEmpty()) {
            return result;
        } else if (other instanceof Set && other.size() < result.cardinality()) {
            for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i+1)) {
                if (other.contains(parent.get(i)))
                    result.clear(i);
            }
        } else {
            bs = new BitSet();
            for (Object v : other) {
                int idx = parent.indexOf(v);
                if (idx >= 0)
                    bs.set(idx);
            }
            result.andNot(bs);
        }
        return result;
    }

    public static @Nonnull <T> BitSet subtract(@Nonnull List<? extends T> parent,
                                               @Nonnull BitSet result, @Nonnull Predicate<T> p) {
        for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i+1)) {
            if (p.test(parent.get(i)))
                result.clear(i);
        }
        return result;
    }
}
