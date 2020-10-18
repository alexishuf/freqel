package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;

public class DynamicBitset extends AbstractBitset {
    private static final long[] EMPTY_ARRAY = new long[0];
    private long value;
    private long[] rem;
    private int inUse;

    public DynamicBitset() {
        this(0, null, 0);
    }


    public DynamicBitset(long[] data) {
        this(data[0], data, 0);
        if (data.length == 1) {
            rem = null;
        } else {
            System.arraycopy(rem, 1, rem, 0, rem.length-1);
            rem[rem.length-1] = 0;
        }
        updateInUse();
    }

    protected DynamicBitset(long value, long[] rem, int inUse) {
        this.value = value;
        this.rem = rem;
        this.inUse = inUse;
    }

    public static @Nonnull DynamicBitset fromJava(@Nonnull BitSet java) {
        if (java.isEmpty())
            return new DynamicBitset();
        return new DynamicBitset(java.toLongArray());
    }

    public static @Nonnull DynamicBitset fromArray(@Nonnull long[] array) {
        if (array.length == 0)
            return new DynamicBitset(0, null, 0);
        else if (array.length == 1)
            return new DynamicBitset(array[0], null, array[0] == 0 ? 0 : 1);
        else
            return new DynamicBitset(Arrays.copyOf(array, array.length));
    }

    public static @Nonnull DynamicBitset fromLong(long value) {
        return new DynamicBitset(value, null, value == 0 ? 0 : 1);
    }

    private static int wordIndex(int bitIndex) {
        if (bitIndex < 0) throw new IndexOutOfBoundsException("Negative index");
        return bitIndex >> 6;
    }

    private void expandToWord(int wordIndex) {
        if (wordIndex > 0) {
            int required = wordIndex + 1;
            if (rem == null) {
                rem = new long[required + (required & 0x1)];
            } else if (rem.length < required) {
                rem = Arrays.copyOf(rem, Math.max(2 * rem.length, required));
            }
        }
    }

    private int expandingWordIndex(int bitIndex) {
        int i = wordIndex(bitIndex);
        expandToWord(i);
        return i;
    }

    private void updateInUse() {
        if (rem != null) {
            for (int i = rem.length-1; i >= 0; i--) {
                if (rem[i] != 0) {
                    inUse = i+2;
                    return;
                }
            }
        }
        inUse = value == 0 ? 0 : 1;
    }

    private void wordAnd(int i, long rhs) {
        if (i == 0) value    &= rhs;
        else        rem[i-1] &= rhs;
    }
    private void wordOr(int i, long rhs) {
        if (i == 0) value    |= rhs;
        else        rem[i-1] |= rhs;
    }
    private void wordXor(int i, long rhs) {
        if (i == 0) value    ^= rhs;
        else        rem[i-1] ^= rhs;
    }

    @Override public void flip(int index) {
        int i = expandingWordIndex(index);
        long mask = 1L << index;
        wordXor(i, mask);
        updateInUse();
    }

    @Override public void flip(int from, int to) {
        if (from == to) return;
        int last = expandingWordIndex(to-1), first = wordIndex(from);
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            wordXor(first, firstMask & lastMask);
        } else {
            wordXor(first, firstMask);
            for (int i = first; i < last-1; i++)
                rem[i] ^= ALL_BITS;
            rem[last-1] ^= lastMask;
        }
        updateInUse();
    }

    @Override public boolean compareAndSet(int index) {
        int i = expandingWordIndex(index);
        long mask = 1L << index;
        if ((word(i) & mask) == 0) {
            wordOr(i, mask);
            inUse = Math.max(inUse, i+1);
            return true;
        }
        return false;
    }

    @Override public void set(int index) {
        int i = expandingWordIndex(index);
        wordOr(i, 1L << index);
        inUse = Math.max(inUse, i+1);
    }

    @Override public void set(int from, int to) {
        if (from == to) return;
        int last = expandingWordIndex(to-1), first = wordIndex(from);
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            wordOr(first, firstMask & lastMask);
        } else {
            wordOr(first, firstMask);
            for (int i = first; i < last - 1; i++)
                rem[i] |= ALL_BITS;
            rem[last-1] |= lastMask;
        }
        inUse = Math.max(inUse, last+1);
    }

    @Override public void clear(int index) {
        int i = wordIndex(index);
        if (rem != null && i > rem.length) return; //no work
        wordAnd(i, ~(1L << index));
        updateInUse();
    }

    @Override public void clear(int from, int to) {
        if (from == to) return;
        int first = wordIndex(from);
        int last = Math.max(wordIndex(to-1), rem != null ? rem.length : 0);

        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            wordAnd(first, ~(firstMask & lastMask));
        } else {
            wordAnd(first, ~firstMask);
            for (int i = first; i < last - 1; i++)
                rem[i] = 0;
            rem[last] &= ~lastMask;
        }
        updateInUse();
    }

    @Override public void clear() {
        value = inUse = 0;
        if (rem != null)
            Arrays.fill(rem, 0, rem.length, 0);
    }

    @Override public boolean get(int index) {
        long mask = 1L << index;
        int i = wordIndex(index);
        return i < inUse && ( (i == 0 ? value : rem[i-1]) & mask ) != 0;
    }

    @Override public int nextSetBit(int from) {
        int i = wordIndex(from);
        if (i >= inUse) return -1;
        long w = word(i) & (ALL_BITS << from);
        while (true) {
            if (w != 0)
                return i * WORD_BITS + Long.numberOfTrailingZeros(w);
            if (++i >= inUse)
                return -1;
            w = rem[i-1];
        }
    }

    @Override public int nextClearBit(int from) {
        int i = wordIndex(from);
        if (i >= inUse) return from;
        long w = ~word(i) & (ALL_BITS << from);
        while (true) {
            if (w != 0)
                return i * WORD_BITS + Long.numberOfTrailingZeros(w);
            if (++i >= inUse)
                return inUse * WORD_BITS;
            w = ~rem[i-1];
        }
    }

    @Override public int previousSetBit(int from) {
        if (from < 0) return -1;
        int i = wordIndex(from);
        if (i >= inUse)
            return length()-1;
        //noinspection ShiftOutOfRange
        long w = word(i) & (ALL_BITS >>> -(from + 1));
        while (true) {
            if (w != 0)
                return (i+1) * WORD_BITS - 1 - Long.numberOfLeadingZeros(w);
            if (i-- == 0)
                return -1;
            w = word(i);
        }
    }

    @Override public int previousClearBit(int from) {
        if (from < 0) return -1;
        int i = wordIndex(from);
        if (i >= inUse)
            return from;
        //noinspection ShiftOutOfRange
        long w = ~word(i) & (ALL_BITS >>> -(from + 1));
        while (true) {
            if (w != 0)
                return (i+1) * WORD_BITS - 1 - Long.numberOfLeadingZeros(w);
            if (i-- == 0)
                return -1;
            w = ~word(i);
        }
    }

    @Override public int length() {
        if (inUse == 0) return 0;
        return WORD_BITS * (inUse-1) + WORD_BITS - Long.numberOfLeadingZeros(word(inUse-1));
    }

    @Override public boolean isEmpty() {
        return inUse == 0;
    }

    @Override public int cardinality() {
        int sum = Long.bitCount(value);
        for (int i = 0; i < inUse-1; i++)
            sum += Long.bitCount(rem[i]);
        return sum;
    }

    @Override public int size() {
        return WORD_BITS + (rem == null ? 0 : rem.length*WORD_BITS);
    }

    @Override public int words() {
        return inUse;
    }

    @Override public long word(int index) {
        if (index == 0)         return value;
        else if (index < inUse) return rem[index-1];
        else                    return 0;
    }

    @Override public boolean intersects(@Nonnull Bitset other) {
        if (other.isEmpty())                    return false;
        if ((value & other.word(0)) != 0) return true;
        for (int i = 1, end = Math.min(inUse, other.words()); i < end; i++) {
            if ((rem[i-1] & other.word(i)) != 0) return true;
        }
        return false;
    }

    @Override public boolean containsAll(@Nonnull Bitset other) {
        int oWords = other.words();
        if (oWords == 0)
            return true;
        if ((other.word(0) &~ value) != 0) return false;
        for (int i = 1; i < oWords; i++) {
            if ((other.word(i) & ~rem[i-1]) != 0) return false;
        }
        return true;
    }

    @Override public void and(@Nonnull Bitset other) {
        if (other == this) return;
        int oWords = other.words();
        while (inUse > 1 && inUse > oWords)
            rem[--inUse - 1] = 0;
        value &= other.word(0);
        for (int i = 1; i < inUse; i++)
            rem[i-1] &= other.word(i);
        updateInUse();
    }

    @Override public void or(@Nonnull Bitset other) {
        if (other == this) return;
        int oWords = other.words();
        expandToWord(oWords);
        if (oWords > 0)
            value |= other.word(0);
        for (int i = 1; i < oWords; i++)
            rem[i-1] |= other.word(i);
        updateInUse();
    }

    @Override public void assign(@Nonnull Bitset other) {
        if (other == this) return;
        int oWords = other.words();
        expandToWord(oWords);
        if (oWords > 0)
            value = other.word(0);
        for (int i = 1; i < oWords; i++)
            rem[i-1] = other.word(i);
        for (int i = oWords; i < inUse; i++)
            rem[i-1] = 0;
        updateInUse();
    }

    @Override public void xor(@Nonnull Bitset other) {
        int oWords = other.words();
        expandToWord(oWords);
        if (oWords > 0)
            value ^= other.word(0);
        for (int i = 1; i < oWords; i++)
            rem[i - 1] ^= other.word(i);
        updateInUse();
    }

    @Override public void andNot(@Nonnull Bitset other) {
        int common = Math.min(inUse, other.words());
        if (common == 0) return;
        value &= ~other.word(0);
        for (int i = 1; i < common; i++)
            rem[i-1] &= ~other.word(i);
        updateInUse();
    }

    @Override public void and(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandingWordIndex(startBit+bits-1); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = (other.word(j) >>> otherStartBit);
            if (opBits < 64)
                op |= (ALL_BITS << opBits);
            wordAnd(i, (op << startBit) | ~(ALL_BITS << startBit));
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void assign(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandingWordIndex(startBit+bits-1); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            if (opBits < 64) {
                long mask = (1L << opBits) - 1;
                long op = (other.word(j) >>> otherStartBit) & mask;
                wordAnd(i, ~(mask << startBit));
                wordOr(i, op << startBit);
            } else {
                if (i == 0) value    = other.word(j);
                else        rem[i-1] = other.word(j);
            }
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void or(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandingWordIndex(startBit+bits-1);
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op &= ~(ALL_BITS << opBits);
            wordOr(i, op << startBit);
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void xor(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandingWordIndex(startBit+bits-1);
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op &= ~(ALL_BITS << opBits);
            wordXor(i, op << startBit);
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void andNot(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandingWordIndex(startBit+bits-1);
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = ~other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op |= (ALL_BITS << opBits);
            if ((startBit & 0x3f) != 0)
                op = (op << startBit) | ~(ALL_BITS << startBit);
            wordAnd(i, op);
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public @Nonnull Bitset copy() {
        return new DynamicBitset(value, rem == null ? null : Arrays.copyOf(rem, rem.length), inUse);
    }

    @Override public @Nonnull long[] toLongArray() {
        if (inUse == 0) return EMPTY_ARRAY;
        long[] data = new long[inUse];
        data[0] = value;
        if (rem != null)
            System.arraycopy(rem, 0, data, 1, inUse-1);
        return data;
    }
}
