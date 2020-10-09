package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;

public class Bitsets {
    public static @Nonnull Bitset createFixed(int capacity) {
        if ((capacity >> 6) == 0)
            return new LongBitset();
        return new ArrayBitset(capacity);
    }

    public static @Nonnull Bitset create(int capacity) {
        int words = (capacity >> 6) + 1;
        return words == 1 ? new DynamicBitset() : new ArrayBitset(new long[words], 0);
    }

    public static @Nonnull Bitset copy(@Nonnull BitSet java) {
        long[] array = java.toLongArray();
        return new ArrayBitset(array, array.length);
    }

    public static @Nonnull Bitset copy(long[] data) {
        int length = data.length;
        if (length > 1)
            return new ArrayBitset(Arrays.copyOf(data, length));
        else
            return DynamicBitset.fromLong(length == 0 ? 0 : data[0]);
    }

    public static @Nonnull Bitset copy(long[] data, int from, int to) {
        int words = to - from;
        if (words > 1) {
            long[] copy = new long[words];
            System.arraycopy(data, from, copy, 0, words);
            return new ArrayBitset(copy);
        } else {
            return DynamicBitset.fromLong(words == 0 ? 0 : data[from]);
        }
    }

    public static @Nonnull Bitset copyFixed(long[] data) {
        int length = data.length;
        if (length > 1)
            return new ArrayBitset(Arrays.copyOf(data, length));
        else
            return new LongBitset(length == 0 ? 0 : data[0]);
    }

    public static @Nonnull Bitset copyFixed(long[] data, int from, int to) {
        int words = to - from;
        if (words > 1) {
            long[] copy = new long[words];
            System.arraycopy(data, from, copy, 0, words);
            return new ArrayBitset(copy);
        } else {
            return new LongBitset(words == 0 ? 0 : data[from]);
        }
    }

    public static @Nonnull Bitset wrap(@Nonnull BitSet java) {
        return new BitSetDelegate(java);
    }

    public static @Nonnull Bitset wrap(long[] data) {
        return new ArrayBitset(data);
    }
}
