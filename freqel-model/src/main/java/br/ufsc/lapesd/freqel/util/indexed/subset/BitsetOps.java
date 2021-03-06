package br.ufsc.lapesd.freqel.util.indexed.subset;

import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.NotInParentException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class BitsetOps {
    public static @Nullable Bitset getBitset(@Nonnull List<?> parent, @Nonnull Collection<?> c) {
        if (c instanceof IndexSubset) {
            IndexSubset<?> ss = (IndexSubset<?>) c;
            if (ss.getParent() == parent)
                return ss.getBitset();
        }
        return null;
    }

    public static @Nonnull Bitset intersect(@Nonnull List<?> parent, @Nonnull Bitset result,
                                            @Nonnull Collection<?> other) {
        Bitset bs = getBitset(parent, other);
        if (bs == null) {
            bs = Bitsets.createFixed(result.size());
            for (Object o : other) {
                int idx = parent.indexOf(o);
                if (idx >= 0)
                    bs.set(idx);
            }
        }
        result.and(bs);
        return result;
    }

    public static @Nonnull <U> Bitset intersect(@Nonnull List<?> parent, @Nonnull Bitset result,
                                                @Nonnull U value) {
        int idx = parent.indexOf(value);
        boolean had = idx >= 0 && result.get(idx);
        result.clear();
        if (had) result.set(idx);
        return result;
    }

    public static @Nonnull <T> Bitset union(@Nonnull IndexSet<? super T> parent, @Nonnull Bitset result,
                                        @Nonnull Collection<? extends T> other) {
        Bitset bs = getBitset(parent, other);
        if (bs != null) {
            result.or(bs);
        } else {
            for (T v : other)
                setBitChecked(parent, result, v);
        }
        return result;
    }

    public static @Nonnull <T> Bitset subset(@Nonnull IndexSet<? super T> index,
                                             @Nonnull IndexSubset<? super T> subset,
                                             @Nonnull Bitset bits,
                                             @Nonnull Collection<T> collection) {
        Bitset result = Bitsets.create(index.size());
        for (T value : collection)
            addToSubset(index, subset, bits, result, value);
        return result;
    }

    public static @Nonnull <T> Bitset subset(@Nonnull IndexSet<? super T> index,
                                             @Nonnull IndexSubset<? super T> subset,
                                             @Nonnull Bitset bits,
                                             @Nonnull T value) {
        Bitset result = Bitsets.create(index.size());
        addToSubset(index, subset, bits, result, value);
        return result;
    }

    private static <T> void addToSubset(@Nonnull IndexSet<? super T> index,
                                        @Nonnull IndexSubset<? super T> subset,
                                        @Nonnull Bitset bits, @Nonnull Bitset result,
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

    public static @Nonnull <T> Bitset subsetExpanding(@Nonnull IndexSet<? super T> parent,
                                                      @Nonnull Collection<T> values) {
        if (values.isEmpty())
            return Bitsets.create(0);
        Bitset bs = getBitset(parent, values);
        if (bs != null)
            return bs.copy();
        Bitset result = Bitsets.create(parent.size() + values.size());
        for (T v : values)
            result.set(parent.safeAdd(v));
        return result;
    }

    public static @Nonnull <T> Bitset union(@Nonnull List<? extends T> parent,
                                            @Nonnull Bitset result,
                                            @Nonnull Predicate<? super T> predicate) {
        int i = 0;
        for (T o : parent) {
            if (predicate.test(o))
                result.set(i);
            ++i;
        }
        return result;
    }

    public static @Nonnull <T> Bitset union(@Nonnull IndexSet<? super T> parent,
                                            @Nonnull Bitset result, @Nonnull T value) {
        setBitChecked(parent, result, value);
        return result;
    }

    private static <T> void setBitChecked(@Nonnull IndexSet<? super T> parent,
                                          @Nonnull Bitset result, @Nonnull T value) {
        int idx = parent.indexOf(value);
        if (idx >= 0)
            result.set(idx);
        else
            throw new NotInParentException(value, parent);
    }

    public static @Nonnull Bitset complement(@Nonnull List<?> parent, @Nonnull Bitset result) {
        result.flip(0, parent.size());
        return result;
    }

    public static @Nonnull <T> Bitset symDiff(@Nonnull IndexSet<? super T> parent,
                                              @Nonnull Bitset result,
                                              @Nonnull Collection<? extends T> other) {
        Bitset bs = getBitset(parent, other);
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

    public static @Nonnull Bitset subtract(@Nonnull List<?> parent, @Nonnull Bitset result,
                                           @Nonnull Collection<?> other) {
        Bitset bs = getBitset(parent, other);
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
            bs = Bitsets.createFixed(parent.size());
            for (Object v : other) {
                int idx = parent.indexOf(v);
                if (idx >= 0)
                    bs.set(idx);
            }
            result.andNot(bs);
        }
        return result;
    }

    public static @Nonnull <T> Bitset subtract(@Nonnull List<? extends T> parent,
                                               @Nonnull Bitset result, @Nonnull Predicate<T> p) {
        for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i+1)) {
            if (p.test(parent.get(i)))
                result.clear(i);
        }
        return result;
    }
}
