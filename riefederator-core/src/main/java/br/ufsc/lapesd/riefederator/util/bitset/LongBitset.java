package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkArgument;

public class LongBitset extends AbstractBitset {
    public static final @Nonnull LongBitset EMPTY = new LongBitset(0);

    private static final int SIZE = 64;
    private static final long ALL_BITS = 0xffffffffffffffffL;
    private long value;

    public LongBitset() { }

    public LongBitset(long value) { this.value = value; }

    public static @Nonnull LongBitset fromJava(@Nonnull BitSet bs) {
        checkArgument(bs.length() <= SIZE, "bs is larger than a single long");
        return new LongBitset(bs.isEmpty() ? 0 : bs.toLongArray()[0]);
    }

    protected void checkInclusiveIndex(int index) {
        if (index < 0 || index >= SIZE) {
            throw new IndexOutOfBoundsException("Cannot access index " + index +
                                                ". LongBitset holds at most 64 bits");
        }
    }
    protected void checkExclusiveIndex(int index) {
        if (index < 0 || index > SIZE) {
            throw new IndexOutOfBoundsException("Cannot mention index/size " + index +
                                                ". LongBitset holds at most 64 bits");
        }
    }

    @Override public void flip(int index) {
        checkInclusiveIndex(index);
        value ^= (1L << index);
    }

    @Override public void flip(int from, int to) {
        if (from == to) return;
        checkInclusiveIndex(from);
        checkExclusiveIndex(to);
        value ^= (ALL_BITS << from) & (ALL_BITS >>> -to);
    }

    @Override public boolean compareAndSet(int index) {
        checkInclusiveIndex(index);
        long mask = 1L << index;
        if ((value & mask) == 0) {
            value |= mask;
            return true;
        }
        return false;
    }

    @Override public void set(int index) {
        checkInclusiveIndex(index);
        value |= (1L << index);
    }

    @Override public void set(int from, int to) {
        checkInclusiveIndex(from);
        checkExclusiveIndex(from);
        value |= (ALL_BITS << from) & (ALL_BITS >>> -to);
    }

    @Override public void clear(int index) {
        checkInclusiveIndex(index);
        value &= ~(1L << index);
    }

    @Override public void clear(int from, int to) {
        checkInclusiveIndex(from);
        checkExclusiveIndex(to);
        value &= ~((ALL_BITS << from) & (ALL_BITS >>> -to));
    }

    @Override public void clear() {
        value = 0;
    }

    @Override public boolean get(int index) {
        checkInclusiveIndex(index);
        return (value & (1L << index)) != 0;
    }

    @Override public int nextSetBit(int from) {
        if (from >= SIZE) return -1;
        long tmp = value & (ALL_BITS << from);
        return tmp == 0 ? -1 : Long.numberOfTrailingZeros(tmp);
    }

    @Override public int nextClearBit(int from) {
        if (from >= SIZE) return from;
        long tmp = ~value & (ALL_BITS << from);
        return tmp == 0 ? SIZE : Long.numberOfTrailingZeros(tmp);
    }

    @Override public int previousSetBit(int from) {
        if (from < 0 || value == 0) return -1;
        if (from >= SIZE) return length() - 1;
        //noinspection ShiftOutOfRange
        long tmp = value & (ALL_BITS >>> -(from+1));
        return SIZE - 1 - Long.numberOfLeadingZeros(tmp);
    }

    @Override public int previousClearBit(int from) {
        if      (from >= SIZE                 ) return from;
        else if (from < 0 || value == ALL_BITS) return -1;
        //noinspection ShiftOutOfRange
        long tmp = ~value & (ALL_BITS >>> -(from+1));
        return SIZE - 1 - Long.numberOfLeadingZeros(tmp);
    }

    @Override public int length() {
        return value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value);
    }

    @Override public boolean isEmpty() {
        return value == 0;
    }

    @Override public int cardinality() {
        return Long.bitCount(value);
    }

    @Override public int cardinalityBefore(int idx) {
        return idx == 0 ? 0 : Long.bitCount(idx >= 64 ? value : value & (ALL_BITS >>> -idx));
    }

    @Override public int cardinalityFrom(int idx) {
        return idx >= 64 ? 0 : Long.bitCount(value & (ALL_BITS << idx));
    }

    @Override public int size() {
        return SIZE;
    }

    public boolean intersects(@Nonnull LongBitset other) {
        return (value & other.value) != 0;
    }

    public void    and(@Nonnull LongBitset other) { value &=  other.value; }
    public void     or(@Nonnull LongBitset other) { value |=  other.value; }
    public void    xor(@Nonnull LongBitset other) { value ^=  other.value; }
    public void andNot(@Nonnull LongBitset other) { value &= ~other.value; }

    @Override public boolean intersects(@Nonnull Bitset other) {
        if (other instanceof LongBitset) return intersects((LongBitset)other);
        return (value & other.word(0)) != 0;
    }

    @Override public boolean containsAll(@Nonnull Bitset other) {
        if ((other.word(0) & ~value) != 0) return false;
        return other.words() <= 1 || other.length() < WORD_BITS;
    }

    @Override public void and(@Nonnull Bitset other) {
        value &= other.word(0);
    }

    @Override public void or(@Nonnull Bitset other) {
        if (other.words() > 1) throw new IndexOutOfBoundsException("Result too large");
        value |= other.word(0);
    }

    @Override public void assign(@Nonnull Bitset other) {
        if (other.words() > 1) throw new IndexOutOfBoundsException("Result too large");
        value = other.word(0);
    }

    @Override public void xor(@Nonnull Bitset other) {
        if (other.words() > 1) throw new IndexOutOfBoundsException("Result too large");
        value ^= other.word(0);
    }

    @Override public void andNot(@Nonnull Bitset other) {
        value &= ~other.word(0);
    }

    private long getOperand(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        assert bits <= 64;
        assert startBit+bits <= 64;

        int i = otherStartBit >> 6;
        int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) +64 - otherStartBit,
                                             (startBit      & ~0x3f) +64 - startBit   ));
        long op = (other.word(i) >>> otherStartBit);
        if (opBits < 64)
            op &= ~(ALL_BITS << opBits);
        bits -= opBits;
        if (bits > 0)
            op |= (other.word(i+1) & (ALL_BITS << bits)) << opBits;
        return op << startBit;
    }

    @Override public void or(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > 64) throw new IndexOutOfBoundsException("Cannot write past bit 64");
        value |= getOperand(startBit, other, otherStartBit, bits);
    }

    @Override public void and(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > 64) throw new IndexOutOfBoundsException("Cannot write past bit 64");
        long op = getOperand(startBit, other, otherStartBit, bits);
        op |= ~((ALL_BITS << startBit) & (ALL_BITS >>> -(startBit+bits)));
        value &= op;
    }

    @Override public void assign(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > 64) throw new IndexOutOfBoundsException("Cannot write past bit 64");
        value &= ~( (ALL_BITS << startBit) & (ALL_BITS >>> -(startBit+bits)) );
        value |= getOperand(startBit, other, otherStartBit, bits);
    }

    @Override public void xor(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > 64) throw new IndexOutOfBoundsException("Cannot write past bit 64");
        value ^= getOperand(startBit, other, otherStartBit, bits);
    }

    @Override public void andNot(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > 64) throw new IndexOutOfBoundsException("Cannot write past bit 64");
        value &= ~getOperand(startBit, other, otherStartBit, bits);
    }

    @Override public @Nonnull Bitset copy() {
        return new LongBitset(value);
    }

    @Override public @Nonnull BitSet toBitSet() {
        return BitSet.valueOf(new long[]{value});
    }

    @Override public @Nonnull long[] toLongArray() {
        return new long[] {value};
    }

    @Override public @Nonnull Bitset createAnd(@Nonnull Bitset other) {
        return new LongBitset(value & other.word(0));
    }

    @Override public @Nonnull Bitset createOr(@Nonnull Bitset other) {
        return other.words() > 1 ? other.createOr(this)
                                 : new LongBitset(value | other.word(0));
    }

    @Override public @Nonnull Bitset createXor(@Nonnull Bitset other) {
        return other.words() > 1 ? other.createXor(this)
                                 : new LongBitset(value ^ other.word(0));
    }

    @Override public @Nonnull Bitset createAndNot(@Nonnull Bitset other) {
        return other.words() > 1 ? other.createAndNot(this)
                                 : new LongBitset(value & ~other.word(0));
    }

    @Override public int words() {
        return 1;
    }

    @Override public long word(int index) {
        return index > 0 ? 0 : value;
    }

    public int hashCode() {
        long h = 1234 ^ value;
        return (int)((h >> 32) ^ h);
    }

    public boolean equals(Object o) {
        if (o instanceof LongBitset) return value == ((LongBitset) o).value;
        if (!(o instanceof Bitset)) return false;

        Bitset bs = (Bitset) o;
        int words = bs.words();
        if (words == 0 && value == 0) return true;
        if (value != bs.word(0)) return false;
        for (int i = 1; i < words; i++) {
            if (bs.word(i) != 0) return false;
        }
        return true;
    }
}
