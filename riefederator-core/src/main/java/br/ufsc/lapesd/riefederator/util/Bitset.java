package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.util.BitSet;

public interface Bitset {
    void flip(int index);
    void flip(int from, int to);

    boolean compareAndSet(int index);
    void set(int index);
    void set(int index, boolean value);
    void set(int index, int to);
    void set(int index, int to, boolean value);

    void clear(int index);
    void clear(int from, int to);
    void clear();
    boolean get(int index);

    int nextSetBit(int from);
    int nextClearBit(int from);
    int previousSetBit(int from);
    int previousClearBit(int from);

    int length();
    boolean isEmpty();
    int cardinality();
    int size();
    int words();
    long word(int index);

    boolean intersects(@Nonnull Bitset other);
    boolean containsAll(@Nonnull Bitset other);

    void and(@Nonnull Bitset other);
    void assign(@Nonnull Bitset other);
    void or(@Nonnull Bitset other);
    void xor(@Nonnull Bitset other);
    void andNot(@Nonnull Bitset other);

    @Nonnull Bitset copy();
    @Nonnull BitSet toBitSet();
    @Nonnull long[] toLongArray();

    @Nonnull Bitset createAnd(@Nonnull Bitset other);
    @Nonnull Bitset createOr(@Nonnull Bitset other);
    @Nonnull Bitset createXor(@Nonnull Bitset other);
    @Nonnull Bitset createAndNot(@Nonnull Bitset other);
}
