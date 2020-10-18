package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;

public class ArrayBitset extends AbstractBitset {
    private static final long[] EMPTY_ARRAY = new long[0];

    private int inUse;
    private long[] words;

    public ArrayBitset() {
        this(EMPTY_ARRAY, 0);
    }

    public ArrayBitset(int bitsCapacity) {
        this(new long[bitsCapacity >> 6], 0);
    }

    public ArrayBitset(long[] words) {
        this(words, 0);
        updateInUse();
    }

    protected ArrayBitset(long[] words, int inUse) {
        this.inUse = inUse;
        this.words = words;
    }

    public static @Nonnull ArrayBitset fromJava(@Nonnull BitSet other) {
        return new ArrayBitset(other.toLongArray());
    }


    private static int wordIndex(int bitIndex) {
        return bitIndex >> 6;
    }
    private boolean expandTo(int wordIdx) {
        int required = wordIdx + 1;
        if (inUse < required) {
            if (words.length < required)
                words = Arrays.copyOf(words, Math.max(2*words.length, required));
            inUse = required;
            return true;
        }
        return false;
    }

    private void updateInUse() {
        for (int i = words.length-1; i >= 0; i--) {
            if (words[i] != 0) {
                inUse = i+1;
                assert selfTest();
                return;
            }
        }
        inUse = 0;
        assert selfTest();
    }

    private boolean checkRange(int from, int to)  {
        if (from < 0 || to < 0)
            throw new IndexOutOfBoundsException("Negative index");
        if (from > to)
            throw new IndexOutOfBoundsException("from="+from+" > to="+to);
        return from < to;
    }
    private void checkIndex(int index) {
        if (index < 0) throw new IndexOutOfBoundsException("Negative index");
    }

    private boolean selfTest() {
        int actualInUse = 0;
        for (int i = words.length - 1; i >= 0; i--) {
            if (words[i] != 0) {
                actualInUse = i+1;
                break;
            }
        }
        assert inUse == actualInUse;
        assert length() <= size();
        assert cardinality() <= length();
        assert length() == 0 || get(length()-1);
        assert !get(length());
        assert !get(size());
        assert !get(size() * 4);
        return true;
    }

    @Override public void flip(int bitIndex) {
        if (bitIndex < 0) throw new IndexOutOfBoundsException("Negative index");
        int wordIndex = wordIndex(bitIndex);
        boolean setInUse = expandTo(wordIndex);
        words[wordIndex] ^= (1L << bitIndex);
        if (!setInUse) updateInUse();
        assert selfTest();
    }

    @Override public void flip(int from, int to) {
        if (!checkRange(from, to)) return; //empty range

        int first = wordIndex(from), last = wordIndex(to-1);
        boolean setInUse = expandTo(last);
        long firstMask = ALL_BITS << from;
        long lastMask  = ALL_BITS >>> -to;
        if (first == last) {
            words[first] ^= (firstMask & lastMask);
        } else {
            words[first] ^= firstMask;
            for (int i = first+1; i < last; i++)
                words[i] ^= ALL_BITS;
            words[last] ^= lastMask;
        }
        if (!setInUse) updateInUse();
        assert selfTest();
    }

    @Override public boolean compareAndSet(int index) {
        checkIndex(index);
        int wordIdx = wordIndex(index);
        expandTo(wordIdx);
        long mask = 1L << index;
        if ((words[wordIdx] & mask) == 0) {
            words[wordIdx] |= mask;
            return true;
        }
        return false;
    }

    @Override public void set(int index) {
        checkIndex(index);
        int wordIdx = wordIndex(index);
        expandTo(wordIdx);
        words[wordIdx] |= (1L << index);
        assert selfTest();
    }


    @Override public void set(int from, int to) {
        if (!checkRange(from, to)) return;
        int first = wordIndex(from), last = wordIndex(to-1);
        expandTo(last);
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            words[first] |= (firstMask & lastMask);
        } else {
            words[first] |= firstMask;
            for (int i = first+1; i < last; i++)
                words[i] |= ALL_BITS;
            words[last] |= lastMask;
        }
        assert selfTest();
    }

    @Override public void clear(int index) {
        checkIndex(index);
        int wordIdx = wordIndex(index);
        if (wordIdx <= inUse) {
            words[wordIdx] &= ~(1L << index);
            updateInUse();
        } else {
            assert selfTest();
        }
    }

    @Override public void clear(int from, int to) {
        if (!checkRange(from, to)) return;
        int first = wordIndex(from), last = wordIndex(to-1);
        if (first > inUse)
            return; //no work
        if (last > inUse) {
            to = length();
            last = inUse;
        }
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            words[first] &= ~(firstMask & lastMask);
        } else {
            words[first] &= ~firstMask;
            for (int i = first+1; i < last; i++)
                words[i] = 0;
            words[last] &= ~lastMask;
        }
        updateInUse();
    }

    @Override public void clear() {
        Arrays.fill(words, 0);
        inUse = 0;
        assert selfTest();
    }

    @Override public boolean get(int index) {
        checkIndex(index);
        int wordIdx = wordIndex(index);
        return wordIdx < inUse && (words[wordIdx] & (1L << index)) != 0;
    }

    @Override public int nextSetBit(int from) {
        checkIndex(from);
        int i = wordIndex(from);
        if (i >= inUse) return -1;

        long w = words[i] & (ALL_BITS << from);
        while (true) {
            if (w != 0)
                return (i * WORD_BITS) + Long.numberOfTrailingZeros(w);
            if (++i == inUse)
                return -1;
            w = words[i];
        }
    }

    @Override public int nextClearBit(int from) {
        checkIndex(from);
        int i = wordIndex(from);
        if (i >= inUse) return from;

        long w = ~words[i] & (ALL_BITS << from);
        while (true) {
            if (w != 0)
                return (i * WORD_BITS) + Long.numberOfTrailingZeros(w);
            if (++i == inUse)
                return inUse * WORD_BITS;
            w = ~words[i];
        }
    }

    @Override public int previousSetBit(int from) {
        if (from < 0)
            return -1;
        int i = wordIndex(from);
        if (i >= inUse)
            return length()-1;
        //noinspection ShiftOutOfRange
        long w = words[i] & (ALL_BITS >>> -(from + 1));
        while (true) {
            if (w != 0)
                return (i+1) * WORD_BITS - 1 - Long.numberOfLeadingZeros(w);
            if (i-- == 0)
                return -1;
            w = words[i];
        }
    }

    @Override public int previousClearBit(int from) {
        if (from < 0)
            return -1;
        int i = wordIndex(from);
        if (i >= inUse)
            return from;
        //noinspection ShiftOutOfRange
        long w = ~words[i] & (ALL_BITS >>> -(from + 1));
        while (true) {
            if (w != 0)
                return (i+1) * WORD_BITS - 1 - Long.numberOfLeadingZeros(w);
            if (i-- == 0)
                return -1;
            w = ~words[i];
        }
    }

    @Override public int length() {
        if (inUse == 0) return 0;
        return WORD_BITS * (inUse-1) + (WORD_BITS - Long.numberOfLeadingZeros(words[inUse-1]));
    }

    @Override public boolean isEmpty() {
        return inUse == 0;
    }

    @Override public int cardinality() {
        int sum = 0;
        for (int i = 0; i < inUse; i++)
            sum += Long.bitCount(words[i]);
        return sum;
    }

    @Override public int size() {
        return words.length*WORD_BITS;
    }

    @Override public int words() {
        return inUse;
    }

    @Override public long word(int index) {
        return index >= inUse ? 0 : words[index];
    }

    @Override public boolean intersects(@Nonnull Bitset other) {
        for (int i = 0, end = Math.min(inUse, other.words()); i < end; i++) {
            if ((words[i] & other.word(i)) != 0)
                return true;
        }
        return false;
    }

    @Override public void and(@Nonnull Bitset other) {
        if (other == this) return;
        int oWords = other.words();
        while (inUse > oWords)
            words[--inUse] = 0;
        for (int i = 0; i < inUse; i++)
            words[i] &= other.word(i);
        updateInUse();
    }

    @Override public void or(@Nonnull Bitset other) {
        if (other == this) return;
        int otherWords = other.words();
        expandTo(otherWords);
        int common = Math.min(inUse, otherWords);
        for (int i = 0; i < common; i++)
            words[i] |= other.word(i);
        for (int i = common; i < otherWords; i++)
            words[i] = other.word(i);
        updateInUse();
        assert selfTest();
    }

    @Override public void assign(@Nonnull Bitset other) {
        if (other == this) return;
        int otherWords = other.words();
        expandTo(otherWords);
        for (int i = 0; i < otherWords; i++)
            words[i] = other.word(i);
        for (int i = otherWords; i < inUse; i++)
            words[i] = 0;
        updateInUse();
        assert selfTest();
    }

    @Override public void xor(@Nonnull Bitset other) {
        int otherWords = other.words();
        expandTo(otherWords);
        int common = Math.min(inUse, otherWords);
        for (int i = 0; i < common; i++)
            words[i] ^= other.word(i);
        for (int i = common; i < otherWords; i++)
            words[i] = other.word(i);
        updateInUse();
    }

    @Override public void andNot(@Nonnull Bitset other) {
        int common = Math.min(words.length, other.words());
        for (int i = 0; i < common; i++)
            words[i] &= ~other.word(i);
        updateInUse();
    }

    @Override public void and(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandTo(wordIndex(startBit+bits-1)); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                                                 (startBit      & ~0x3f) + 64 - startBit));
            long op = (other.word(j) >>> otherStartBit);
            if (opBits < 64)
                op |= (ALL_BITS << opBits);
            words[i] &= (op << startBit) | ~(ALL_BITS << startBit);
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void assign(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandTo(wordIndex(startBit+bits-1)); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            if (opBits < 64) {
                long mask = (1L << opBits) - 1;
                long op = (other.word(j) >>> otherStartBit) & mask;
                words[i] &= ~(mask << startBit);
                words[i] |= op << startBit;
            } else {
                words[i] = other.word(j);
            }
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void or(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandTo(wordIndex(startBit+bits-1)); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                                                 (startBit      & ~0x3f) + 64 - startBit));
            long op = other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op &= ~(ALL_BITS << opBits);
            words[i] |= op << startBit;
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void xor(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandTo(wordIndex(startBit+bits-1)); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                                                 (startBit      & ~0x3f) + 64 - startBit));
            long op = other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op &= ~(ALL_BITS << opBits);
            words[i] ^= op << startBit;
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public void andNot(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        expandTo(wordIndex(startBit+bits-1)); //grow, if needed
        while (bits > 0) {
            int i = wordIndex(startBit), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                                                 (startBit      & ~0x3f) + 64 - startBit));
            long op = ~other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op |= (ALL_BITS << opBits);
            if ((startBit & 0x3f) != 0)
                op = (op << startBit) | ~(ALL_BITS << startBit);
            words[i] &= op;
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
        updateInUse();
    }

    @Override public @Nonnull Bitset copy() {
        long[] data = words.length == 0 ? words : Arrays.copyOf(words, words.length);
        return new ArrayBitset(data, inUse);
    }

    @Override public @Nonnull BitSet toBitSet() {
        return BitSet.valueOf(words);
    }

    @Override public @Nonnull long[] toLongArray() {
        return Arrays.copyOf(words, inUse);
    }
}
