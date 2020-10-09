package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;

public class BitSetDelegate extends AbstractBitset {
    private final @Nonnull BitSet delegate;

    public BitSetDelegate() {
        this(new BitSet());
    }

    public BitSetDelegate(@Nonnull BitSet delegate) {
        this.delegate = delegate;
    }

    @Override public void flip(int index) {
        delegate.flip(index);
    }

    @Override public void flip(int from, int to) {
        delegate.flip(from, to);
    }

    @Override public boolean compareAndSet(int index) {
        if (!delegate.get(index)) {
            delegate.set(index);
            return true;
        }
        return false;
    }

    @Override public void set(int index) {
        delegate.set(index);
    }

    @Override public void set(int index, boolean value) {
        delegate.set(index, value);
    }

    @Override public void set(int index, int to) {
        delegate.set(index, to);
    }

    @Override public void set(int index, int to, boolean value) {
        delegate.set(index, to, value);
    }

    @Override public void clear(int index) {
        delegate.clear(index);
    }

    @Override public void clear(int from, int to) {
        delegate.clear(from, to);
    }

    @Override public void clear() {
        delegate.clear();
    }

    @Override public boolean get(int index) {
        return delegate.get(index);
    }

    @Override public int nextSetBit(int from) {
        return delegate.nextSetBit(from);
    }

    @Override public int nextClearBit(int from) {
        return delegate.nextClearBit(from);
    }

    @Override public int previousSetBit(int from) {
        return delegate.previousSetBit(from);
    }

    @Override public int previousClearBit(int from) {
        return delegate.previousClearBit(from);
    }

    @Override public int length() {
        return delegate.length();
    }

    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override public int cardinality() {
        return delegate.cardinality();
    }

    @Override public int size() {
        return delegate.size();
    }

    @Override public int words() {
        return delegate.isEmpty() ? 0 : ((delegate.length()-1) >> 6) + 1;
    }

    @Override public long word(int index) {
        if (index >= words()) return 0;
        return delegate.toLongArray()[index];
    }

    @Override public boolean intersects(@Nonnull Bitset other) {
        if (other instanceof BitSetDelegate)
            return delegate.intersects(((BitSetDelegate) other).delegate);
        long[] mine = delegate.toLongArray();
        for (int i = 0, end = Math.min(mine.length, other.words()); i < end; i++) {
            if ((mine[i] & other.word(i)) != 0) return true;
        }
        return false;
    }

    @Override public boolean containsAll(@Nonnull Bitset other) {
        long[] mine = toLongArray();
        for (int i = 0; i < mine.length; i++) {
            if ((other.word(i) & ~mine[i]) != 0) return false;
        }
        for (int i = mine.length, oWords = other.words(); i < oWords; i++) {
            if (other.word(i) != 0) return false;
        }
        return true;
    }

    @Override public void and(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        int otherWords = other.words();
        for (int i = 0, end = Math.min(mine.length, otherWords); i < end; i++)
            mine[i] &= other.word(i);
        if (mine.length > otherWords)
            Arrays.fill(mine, otherWords, mine.length, 0);
        delegate.clear();
        delegate.or(BitSet.valueOf(mine));
    }

    @Override public void or(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        int oWords = other.words();
        int end1 = Math.min(mine.length, oWords);
        if (oWords > mine.length)
            mine = Arrays.copyOf(mine, oWords);
        for (int i = 0; i < end1; i++)
            mine[i] |= other.word(i);
        for (int i = end1; i < mine.length; i++)
            mine[i] = other.word(i);
        delegate.clear();
        delegate.or(BitSet.valueOf(mine));
    }

    @Override public void assign(@Nonnull Bitset other) {
        delegate.clear();
        delegate.or(other.toBitSet());
    }

    @Override public void xor(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        int oWords = other.words();
        int end1 = Math.min(mine.length, oWords);
        if (oWords > mine.length)
            mine = Arrays.copyOf(mine, oWords);
        for (int i = 0; i < end1; i++)
            mine[i] ^= other.word(i);
        for (int i = end1; i < mine.length; i++)
            mine[i] = other.word(i);
        delegate.clear();
        delegate.or(BitSet.valueOf(mine));
    }

    @Override public void andNot(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        for (int i = 0, end = Math.min(mine.length, other.words()); i < end; i++)
            mine[i] &= ~other.word(i);
        delegate.clear();
        delegate.or(BitSet.valueOf(mine));
    }

    @Override public @Nonnull Bitset copy() {
        return new BitSetDelegate(toBitSet());
    }

    @Override public @Nonnull BitSet toBitSet() {
        return (BitSet) delegate.clone();
    }

    @Override public @Nonnull long[] toLongArray() {
        return delegate.toLongArray();
    }

    @Override public @Nonnull Bitset createAnd(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        int otherWords = other.words();
        for (int i = 0, end = Math.min(mine.length, otherWords); i <end; i++)
            mine[i] &= other.word(i);
        if (mine.length > otherWords)
            Arrays.fill(mine, otherWords, mine.length, 0);
        return new BitSetDelegate(BitSet.valueOf(mine));
    }

    @Override public @Nonnull Bitset createOr(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        int oWords = other.words();
        int end1 = Math.min(mine.length, oWords);
        if (oWords > mine.length)
            mine = Arrays.copyOf(mine, oWords);
        for (int i = 0; i <end1; i++)
            mine[i] |= other.word(i);
        for (int i = end1; i < mine.length; i++)
            mine[i] = other.word(i);
        return new BitSetDelegate(BitSet.valueOf(mine));
    }

    @Override public @Nonnull Bitset createXor(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        int oWords = other.words();
        int end1 = Math.min(mine.length, oWords);
        if (oWords > mine.length)
            mine = Arrays.copyOf(mine, oWords);
        for (int i = 0; i <end1; i++)
            mine[i] ^= other.word(i);
        for (int i = end1; i < mine.length; i++)
            mine[i] = other.word(i);
        return new BitSetDelegate(BitSet.valueOf(mine));
    }

    @Override public @Nonnull Bitset createAndNot(@Nonnull Bitset other) {
        long[] mine = delegate.toLongArray();
        for (int i = 0, end = Math.min(mine.length, other.words()); i <end; i++)
            mine[i] &= ~other.word(i);
        return new BitSetDelegate(BitSet.valueOf(mine));
    }
}
