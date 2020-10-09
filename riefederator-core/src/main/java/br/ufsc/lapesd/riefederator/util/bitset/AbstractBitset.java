package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.BitSet;

public abstract class AbstractBitset implements Bitset {
    protected static final long ALL_BITS = 0xffffffffffffffffL;
    protected static final int WORD_BITS = 64;

    @Override public void set(int index, boolean value) {
        if (value) set(index);
        else       clear(index);
    }

    @Override public void set(int from, int to, boolean value) {
        if (value) set(from, to);
        else       clear(from, to);
    }

    @Override public @Nonnull BitSet toBitSet() {
        return BitSet.valueOf(toLongArray());
    }

    @Override public boolean containsAll(@Nonnull Bitset other) {
        for (int i = 0, oWords = other.words(); i < oWords; i++) {
            if ((other.word(i) & ~word(i)) != 0) return false;
        }
        return true;
    }

    @Override public @Nonnull Bitset createAnd(@Nonnull Bitset other) {
        Bitset c = copy();
        c.and(other);
        return c;
    }

    @Override public @Nonnull Bitset createOr(@Nonnull Bitset other) {
        Bitset c = copy();
        c.or(other);
        return c;
    }

    @Override public @Nonnull Bitset createXor(@Nonnull Bitset other) {
        Bitset c = copy();
        c.xor(other);
        return c;
    }

    @Override public @Nonnull Bitset createAndNot(@Nonnull Bitset other) {
        Bitset c = copy();
        c.andNot(other);
        return c;
    }

    @Override public int hashCode() {
        long h = 1234;
        for (int i = 0, size = words(); i < size; i++)
            h ^= word(i) * (i + 1);
        return (int)((h >> 32) ^ h);
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof Bitset)) return false;

        Bitset bs = (Bitset) o;
        int mySize = words(), bsSize = bs.words();
        int min = Math.min(mySize, bsSize);
        for (int i = 0; i < min; i++) {
            if (word(i) != bs.word(i)) return false;
        }
        if (mySize > min) {
            for (int i = min; i < mySize; i++) {
                if (word(i) != 0) return false;
            }
        } else if (bsSize > min) {
            for (int i = min; i < bsSize; i++) {
                if (bs.word(i) != 0) return false;
            }
        }
        return true;
    }

    @Override public String toString() {
        StringBuilder b = new StringBuilder("{");
        for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i+1))
            b.append(i).append(", ");
        if (b.length() > 1)
            b.setLength(b.length()-2);
        return b.append('}').toString();
    }
}
