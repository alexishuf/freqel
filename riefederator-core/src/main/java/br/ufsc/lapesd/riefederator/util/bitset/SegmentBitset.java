package br.ufsc.lapesd.riefederator.util.bitset;

import br.ufsc.lapesd.riefederator.util.Bitset;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class SegmentBitset extends AbstractBitset {
    private long[] data;
    private int begin, end, size;

    public SegmentBitset(long[] data, int begin, int end) {
        map(data, begin, end);
    }

    public void map(long[] data, int from, int to) {
        this.data = data;
        this.begin = from;
        this.end = to;
        this.size = (to-from)<<6;
    }

    private void checkInclusive(int bitIndex) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("Negative index="+bitIndex);
        if (bitIndex >= size)
            throw new IndexOutOfBoundsException("index="+bitIndex+" > size");
    }

    private void checkExclusive(int bitIndex) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("Negative index="+bitIndex);
        if (bitIndex >= size)
            throw new IndexOutOfBoundsException("index="+bitIndex+" > size");
    }

    private int wordIndex(int bitIndex) {
        checkInclusive(bitIndex);
        return begin + (bitIndex >> 6);
    }

    private int wordEnd(int bitIndex) {
        checkExclusive(bitIndex);
        return begin + (bitIndex >> 6);
    }

    @Override public void flip(int index) {
        data[wordIndex(index)] ^= (1L << index);
    }

    @Override public void flip(int from, int to) {
        if (from == to) return;
        int first = wordIndex(from), last = wordIndex(to-1);
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            data[first] ^= (firstMask & lastMask);
        } else {
            data[first] ^= firstMask;
            for (int i = first+1; i < last; i++) data[i] ^= ALL_BITS;
            data[last] ^= lastMask;
        }
    }

    @Override public boolean compareAndSet(int index) {
        int i = wordIndex(index);
        long mask = 1L << index;
        if ((data[i] & mask) == 0) {
            data[i] |= mask;
            return true;
        }
        return false;
    }

    @Override public void set(int index) {
        data[wordIndex(index)] |= (1L << index);
    }

    @Override public void set(int from, int to) {
        if (from == to) return;
        int first = wordIndex(from), last = wordIndex(to-1);
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            data[first] |= (firstMask & lastMask);
        } else {
            data[first] |= firstMask;
            for (int i = first+1; i < last; i++) data[i] |= ALL_BITS;
            data[last] |= lastMask;
        }
    }

    @Override public void clear(int index) {
        data[wordIndex(index)] &= ~(1L << index);
    }

    @Override public void clear(int from, int to) {
        if (from == to) return;
        int first = wordIndex(from), last = wordIndex(to-1);
        long firstMask = ALL_BITS << from, lastMask = ALL_BITS >>> -to;
        if (first == last) {
            data[first] &= ~(firstMask & lastMask);
        } else {
            data[first] &= ~firstMask;
            for (int i = first+1; i < last; i++) data[i] = 0;
            data[last] &= lastMask;
        }
    }

    @Override public void clear() {
        Arrays.fill(data, begin, end, 0);
    }

    @Override public boolean get(int index) {
        return (data[wordIndex(index)] & (1L << index)) != 0;
    }

    @Override public int nextSetBit(int from) {
        if (from >= size)
            return -1;
        int i = wordIndex(from);
        long w = data[i] & (ALL_BITS << from);
        while (true) {
            if (w != 0)
                return (i-begin)*WORD_BITS + Long.numberOfTrailingZeros(w);
            if (++i == end)
                return -1;
            w = data[i];
        }
    }

    @Override public int nextClearBit(int from) {
        if (from >= size) return from;
        int i = wordIndex(from);
        long w = ~data[i] & (ALL_BITS << from);
        while (true) {
            if (w != 0)
                return (i-begin)*WORD_BITS + Long.numberOfTrailingZeros(w);
            if (++i == end)
                return (end-begin) * WORD_BITS;
            w = ~data[i];
        }
    }

    @Override public int previousSetBit(int from) {
        if (from < 0) return -1;
        if (from >= size) return length()-1;
        int i = wordIndex(from);
        //noinspection ShiftOutOfRange
        long w = data[i] & ALL_BITS >>> -(from+1);
        while (true) {
            if (w != 0)
                return (i-begin+1)*WORD_BITS - 1 - Long.numberOfLeadingZeros(w);
            if (i-- == 0)
                return -1;
            w = data[i];
        }
    }

    @Override public int previousClearBit(int from) {
        if (from < 0) return -1;
        if (from >= size) return from;
        int i = wordIndex(from);
        //noinspection ShiftOutOfRange
        long w = ~data[i] & ALL_BITS >>> -(from+1);
        while (true) {
            if (w != 0)
                return (i-begin+1)*WORD_BITS - 1 - Long.numberOfLeadingZeros(w);
            if (i-- == 0)
                return -1;
            w = ~data[i];
        }
    }

    @Override public int length() {
        for (int i = end-1; i >= begin; i--) {
            if (data[i] != 0)
                return (i-begin)*WORD_BITS + WORD_BITS - Long.numberOfLeadingZeros(data[i]);
        }
        return 0;
    }

    @Override public boolean isEmpty() {
        for (int i = begin; i < end; i++) {
            if (data[i] != 0) return false;
        }
        return true;
    }

    @Override public int cardinality() {
        int sum = 0;
        for (int i = begin; i < end; i++)
            sum += Long.bitCount(data[i]);
        return sum;
    }

    @Override public int cardinalityBefore(int idx) {
        if      (idx == 0   ) return 0;
        else if (idx >= size) return cardinality();
        int last = begin + ((idx-1) >> 6);
        int sum = 0;
        for (int i = begin; i < last; i++)
            sum += Long.bitCount(data[i]);
        return sum + Long.bitCount(data[last] & (ALL_BITS >>> -idx));
    }

    @Override public int cardinalityFrom(int idx) {
        int first = begin + (idx >> 6);
        if (first > end) return 0;
        int sum = Long.bitCount(data[first] & (ALL_BITS << idx));
        for (int i = first+1; i < end; i++)
            sum += Long.bitCount(data[i]);
        return sum;
    }

    @Override public int size() {
        return size;
    }

    @Override public int words() {
        return end-begin;
    }

    @Override public long word(int index) {
        int i = begin + index;
        return i >= end ? 0 : data[i];
    }

    @Override public boolean intersects(@Nonnull Bitset other) {
        int common = Math.min(end - begin, other.words());
        for (int i = 0; i < common; i++) {
            if ((data[begin+i] & other.word(i)) != 0) return true;
        }
        return false;
    }

    @Override public void and(@Nonnull Bitset other) {
        if (other == this) return;
        int oWords = other.words();
        for (int i = end-1, stop = begin+oWords; i >= stop; i--)
            data[i] = 0;
        for (int i = 0, common = Math.min(end-begin, oWords); i < common; i++)
            data[begin+i] &= other.word(i);
    }

    @Override public void or(@Nonnull Bitset other) {
        int oWords = other.words();
        int common = Math.min(end-begin, oWords);
        for (int i = 0; i < common; i++)
            data[begin+i] |= other.word(i);
        for (int i = common; i < oWords; i++) {
            if (other.word(i) != 0)
                throw new IndexOutOfBoundsException("other bitset has bits above size()");
        }
    }

    @Override public void assign(@Nonnull Bitset other) {
        int oWords = other.words();
        for (int i = 0; i < oWords; i++) {
            int wIdx = begin + i;
            if (wIdx >= end && other.word(i) != 0)
                throw new IndexOutOfBoundsException("other bitset has bits above size()");
            data[wIdx] = other.word(i);
        }
        for (int i = begin+oWords; i < end; i++)
            data[i] = 0;
    }

    @Override public void xor(@Nonnull Bitset other) {
        int oWords = other.words();
        int common = Math.min(end - begin, oWords);
        for (int i = 0; i < common; i++)
            data[begin+i] ^= other.word(i);
        for (int i = common; i < oWords; i++) {
            if (other.word(i) != 0)
                throw new IndexOutOfBoundsException("other bitset has bits above size()");
        }
    }

    @Override public void andNot(@Nonnull Bitset other) {
        int oWords = other.words();
        int common = Math.min(end - begin, oWords);
        for (int i = 0; i < common; i++)
            data[begin+i] &= ~other.word(i);
    }

    @Override public void and(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > size) throw new IndexOutOfBoundsException("startBits+bits > size");
        while (bits > 0) {
            int i = startBit >> 6, j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                                                 (startBit      & ~0x3f) + 64 - startBit));
            long op = (other.word(j) >>> otherStartBit);
            if (opBits < 64)
                op |= (ALL_BITS << opBits);
            data[begin+i] &= (op << startBit) | ~(ALL_BITS << startBit);
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
    }

    @Override public void assign(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > size) throw new IndexOutOfBoundsException("startBits+bits > size");
        while (bits > 0) {
            int i = begin + (startBit >> 6), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                                                 (startBit      & ~0x3f) + 64 - startBit));
            if (opBits < 64) {
                long mask = (1L << opBits) - 1;
                long op = (other.word(j) >>> otherStartBit) & mask;
                data[i] &= ~(mask << startBit);
                data[i] |= op << startBit;
            } else {
                data[i] = other.word(j);
            }
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
    }

    @Override public void or(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > size) throw new IndexOutOfBoundsException("startBits+bits > size");
        while (bits > 0) {
            int i = begin + (startBit >> 6), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op &= ~(ALL_BITS << opBits);
            data[i] |= op << startBit;
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
    }

    @Override public void xor(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > size) throw new IndexOutOfBoundsException("startBits+bits > size");
        while (bits > 0) {
            int i = begin + (startBit >> 6), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op &= ~(ALL_BITS << opBits);
            data[i] ^= op << startBit;
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
    }

    @Override public void andNot(int startBit, @Nonnull Bitset other, int otherStartBit, int bits) {
        if (startBit+bits > size) throw new IndexOutOfBoundsException("startBits+bits > size");
        while (bits > 0) {
            int i = begin + (startBit >> 6), j = otherStartBit >> 6;
            int opBits = Math.min(bits, Math.min((otherStartBit & ~0x3f) + 64 - otherStartBit,
                    (startBit      & ~0x3f) + 64 - startBit));
            long op = ~other.word(j) >>> otherStartBit;
            if (opBits < 64)
                op |= (ALL_BITS << opBits);
            if ((startBit & 0x3f) != 0)
                op = (op << startBit) | ~(ALL_BITS << startBit);
            data[i] &= op;
            startBit      += opBits;
            otherStartBit += opBits;
            bits          -= opBits;
        }
    }

    @Override public @Nonnull Bitset copy() {
        return new ArrayBitset(Arrays.copyOfRange(data, begin, end));
    }

    @Override public @Nonnull long[] toLongArray() {
        return Arrays.copyOfRange(data, begin, end);
    }
}
