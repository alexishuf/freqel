package br.ufsc.lapesd.riefederator.util;

import br.ufsc.lapesd.riefederator.util.bitset.SegmentBitset;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class RawAlignedBitSet {
    private static long ALL_ON = 0xffffffffffffffffL;
    private int[] s;
    private int totalWords;
    private long[] garbage = null;

    public RawAlignedBitSet(int... bitsPerComponent) {
        totalWords = 0;
        s = new int[bitsPerComponent.length+1];
        for (int i = 0, nComponents = bitsPerComponent.length; i < nComponents; i++) {
            s[i] = totalWords;
            totalWords += wordsRequired(bitsPerComponent[i]);
        }
        s[bitsPerComponent.length] = totalWords;
    }

    private static int wordIndex(int bit) {
        return bit >> 6;
    }

    public static int wordsRequired(int nBits) {
        return wordIndex(nBits - 1) + 1;
    }

    public long[] alloc() {
        if (garbage != null) {
            long[] array = garbage;
            Arrays.fill(array, 0);
            garbage = null;
            return array;
        }
        return new long[totalWords];
    }

    public void dealloc(long[] array) {
        garbage = array;
    }

    public int getComponentCount() {
        return s.length-1;
    }

    public int getBitCount(int component) {
        return s[component+1] - s[component];
    }

    public int componentBegin(int component) { return s[component  ]; }
    public int componentEnd  (int component) { return s[component+1]; }
    public int componentWords(int component) { return s[component+1] - s[component]; }

    public boolean get(long[] a, int comp, int index) {
        return ( a[s[comp] + wordIndex(index)] & (1L << index) ) != 0;
    }
    public void set(long[] a, int comp, int index) {
        a[s[comp] + wordIndex(index)] |= (1L << index);
    }
    public void unset(long[] a, int comp, int index) {
        a[s[comp] + wordIndex(index)] &= ~(1L << index);
    }

    public static boolean get(long[] a, int index) {
        return ( a[wordIndex(index)] & (1L << index) ) != 0;
    }
    public static void set(long[] a, int index) {
        a[wordIndex(index)] |= (1L << index);
    }
    public static void unset(long[] a, int index) {
        a[wordIndex(index)] &= ~(1L << index);
    }

    public static void clear(long[] a) { Arrays.fill(a, 0); }
    public void clear(long[] a, int comp) { Arrays.fill(a, s[comp], s[comp+1], 0); }
    public void clearFrom(long[] a, int comp) { Arrays.fill(a, s[comp], a.length, 0); }

    public static boolean isEmpty(long[] a) { return isEmpty(a, 0, a.length); }
    public boolean isEmpty(long[] a, int comp) { return isEmpty(a, s[comp], s[comp+1]); }
    public static boolean isEmpty(long[] a, int beginWord, int endWord) {
        for (int i = beginWord; i < endWord; i++) {
            if (a[i] != 0) return false;
        }
        return true;
    }

    public static boolean and(long[] a, long[] b) { return and(a, 0, a.length, a, 0, b, 0); }
    public static boolean and(long[] o, long[] a, long[] b) { return and(o, 0, o.length, a, 0, b, 0); }
    public boolean and(int comp, long[] a, long[] b) { return and(a, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean and(long[] a, int comp, long[] b) { return and(a, s[comp], s[comp+1], a, s[comp], b, 0); }

    public boolean and(long[] a, long[] b, int comp) { return and(a, 0, a.length, a, 0, b, s[comp]); }
    public boolean and(int comp, long[] o, long[] a, long[] b) { return and(o, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean and(long[] o, int comp, long[] a, long[] b) { return and(o, s[comp], s[comp+1], a, 0, b, 0); }
    public boolean and(long[] o, long[] a, int comp, long[] b) { return and(o, 0, s[comp+1]-s[comp], a, s[comp], b, 0); }
    public boolean and(long[] o, long[] a, long[] b, int comp) { return and(o, 0, s[comp+1]-s[comp], a, 0, b, s[comp]); }
    public boolean and(long[] o, long[] a, int aComp, long[] b, int bComp) {
        assert s[aComp+1]-s[aComp] == s[bComp+1]-s[bComp]
                : "a and b components have mismatching word lengths";
        int words = Math.min(s[aComp + 1] - s[aComp], s[bComp + 1] - s[bComp]);
        return and(o, 0, words, a, s[aComp], b, s[bComp]);
    }

    public void and(long[] o, long[] a, @Nonnull Bitset b) {and(o, 0, o.length, a, 0, b);}
    public void and(long[] o, long[] a, int aComp, @Nonnull Bitset b) {and(o, 0, Math.max(o.length, s[aComp+1]), a, s[aComp], b);}
    public void and(long[] o, int comp, long[] a, int aComp, @Nonnull Bitset b) {and(o, s[comp], s[comp+1], a, s[aComp], b);}
    public void and(int comp, long[] o, long[] a, @Nonnull Bitset b) {and(o, s[comp], s[comp+1], a, s[comp], b);}
    public void and(long[] a, @Nonnull Bitset b) {and(a, 0, a.length, a, 0, b);}
    public void and(long[] a, int aComp, @Nonnull Bitset b) {and(a, s[aComp], s[aComp+1], a, s[aComp], b);}

    public static boolean intersects(long[] a, long[] b) { return intersects(a, 0, a.length, b, 0); }
    public        boolean intersects(int comp, long[] a, long[] b) { return intersects(a, s[comp], s[comp+1], b, s[comp]); }
    public        boolean intersects(long[] a, int comp, long[] b) { return intersects(a, s[comp], s[comp+1], b, 0); }
    public        boolean intersects(long[] a, long[] b, int comp) { return intersects(a, 0, a.length, b, s[comp]); }

    public static boolean or(long[] a, long[] b) { return or(a, 0, a.length, a, 0, b, 0); }
    public static boolean or(long[] o, long[] a, long[] b) { return or(o, 0, o.length, a, 0, b, 0); }
    public boolean or(int comp, long[] a, long[] b) { return or(a, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean or(long[] a, int comp, long[] b) { return or(a, s[comp], s[comp+1], a, s[comp], b, 0); }

    public boolean or(long[] a, long[] b, int comp) { return or(a, 0, a.length, a, 0, b, s[comp]); }
    public boolean or(int comp, long[] o, long[] a, long[] b) { return or(o, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean or(long[] o, int comp, long[] a, long[] b) { return or(o, s[comp], s[comp+1], a, 0, b, 0); }
    public boolean or(long[] o, long[] a, int comp, long[] b) { return or(o, 0, s[comp+1]-s[comp], a, s[comp], b, 0); }
    public boolean or(long[] o, long[] a, long[] b, int comp) { return or(o, 0, s[comp+1]-s[comp], a, 0, b, s[comp]); }
    public boolean or(long[] o, long[] a, int aComp, long[] b, int bComp) {
        assert s[aComp+1]-s[aComp] == s[bComp+1]-s[bComp]
                : "a and b components have mismatching word lengths";
        int words = Math.min(s[aComp + 1] - s[aComp], s[bComp + 1] - s[bComp]);
        return or(o, 0, words, a, s[aComp], b, s[bComp]);
    }

    public void or(long[] o, long[] a, @Nonnull Bitset b) {or(o, 0, o.length, a, 0, b);}
    public void or(long[] o, long[] a, int aComp, @Nonnull Bitset b) {or(o, 0, Math.max(o.length, s[aComp+1]), a, s[aComp], b);}
    public void or(long[] o, int comp, long[] a, int aComp, @Nonnull Bitset b) {or(o, s[comp], s[comp+1], a, s[aComp], b);}
    public void or(int comp, long[] o, long[] a, @Nonnull Bitset b) {or(o, s[comp], s[comp+1], a, s[comp], b);}
    public void or(long[] a, @Nonnull Bitset b) {or(a, 0, a.length, a, 0, b);}
    public void or(long[] a, int aComp, @Nonnull Bitset b) {or(a, s[aComp], s[aComp+1], a, s[aComp], b);}

    public static boolean andNot(long[] a, long[] b) { return andNot(a, 0, a.length, a, 0, b, 0); }
    public static boolean andNot(long[] o, long[] a, long[] b) { return andNot(o, 0, o.length, a, 0, b, 0); }
    public boolean andNot(int comp, long[] a, long[] b) { return andNot(a, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean andNot(long[] a, int comp, long[] b) { return andNot(a, s[comp], s[comp+1], a, s[comp], b, 0); }

    public boolean andNot(long[] a, long[] b, int comp) { return andNot(a, 0, a.length, a, 0, b, s[comp]); }
    public boolean andNot(int comp, long[] o, long[] a, long[] b) { return andNot(o, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean andNot(long[] o, int comp, long[] a, long[] b) { return andNot(o, s[comp], s[comp+1], a, 0, b, 0); }
    public boolean andNot(long[] o, long[] a, int comp, long[] b) { return andNot(o, 0, s[comp+1]-s[comp], a, s[comp], b, 0); }
    public boolean andNot(long[] o, long[] a, long[] b, int comp) { return andNot(o, 0, s[comp+1]-s[comp], a, 0, b, s[comp]); }
    public boolean andNot(long[] o, long[] a, int aComp, long[] b, int bComp) {
        assert s[aComp+1]-s[aComp] == s[bComp+1]-s[bComp]
                : "a and b components have mismatching word lengths";
        int words = Math.min(s[aComp + 1] - s[aComp], s[bComp + 1] - s[bComp]);
        return andNot(o, 0, words, a, s[aComp], b, s[bComp]);
    }

    public void andNot(long[] o, long[] a, @Nonnull Bitset b) {andNot(o, 0, o.length, a, 0, b);}
    public void andNot(long[] o, long[] a, int aComp, @Nonnull Bitset b) {andNot(o, 0, Math.max(o.length, s[aComp+1]), a, s[aComp], b);}
    public void andNot(long[] o, int comp, long[] a, int aComp, @Nonnull Bitset b) {andNot(o, s[comp], s[comp+1], a, s[aComp], b);}
    public void andNot(int comp, long[] o, long[] a, @Nonnull Bitset b) {andNot(o, s[comp], s[comp+1], a, s[comp], b);}
    public void andNot(long[] a, @Nonnull Bitset b) {andNot(a, 0, a.length, a, 0, b);}
    public void andNot(long[] a, int aComp, @Nonnull Bitset b) {andNot(a, s[aComp], s[aComp+1], a, s[aComp], b);}

    public static boolean xor(long[] a, long[] b) { return xor(a, 0, a.length, a, 0, b, 0); }
    public static boolean xor(long[] o, long[] a, long[] b) { return xor(o, 0, o.length, a, 0, b, 0); }
    public boolean xor(int comp, long[] a, long[] b) { return xor(a, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean xor(long[] a, int comp, long[] b) { return xor(a, s[comp], s[comp+1], a, s[comp], b, 0); }

    public boolean xor(long[] a, long[] b, int comp) { return xor(a, 0, a.length, a, 0, b, s[comp]); }
    public boolean xor(int comp, long[] o, long[] a, long[] b) { return xor(o, s[comp], s[comp+1], a, s[comp], b, s[comp]); }
    public boolean xor(long[] o, int comp, long[] a, long[] b) { return xor(o, s[comp], s[comp+1], a, 0, b, 0); }
    public boolean xor(long[] o, long[] a, int comp, long[] b) { return xor(o, 0, s[comp+1]-s[comp], a, s[comp], b, 0); }
    public boolean xor(long[] o, long[] a, long[] b, int comp) { return xor(o, 0, s[comp+1]-s[comp], a, 0, b, s[comp]); }
    public boolean xor(long[] o, long[] a, int aComp, long[] b, int bComp) {
        assert s[aComp+1]-s[aComp] == s[bComp+1]-s[bComp]
                : "a and b components have mismatching word lengths";
        int words = Math.min(s[aComp + 1] - s[aComp], s[bComp + 1] - s[bComp]);
        return xor(o, 0, words, a, s[aComp], b, s[bComp]);
    }

    public void xor(long[] o, long[] a, @Nonnull Bitset b) {xor(o, 0, o.length, a, 0, b);}
    public void xor(long[] o, long[] a, int aComp, @Nonnull Bitset b) {xor(o, 0, Math.max(o.length, s[aComp+1]), a, s[aComp], b);}
    public void xor(long[] o, int comp, long[] a, int aComp, @Nonnull Bitset b) {xor(o, s[comp], s[comp+1], a, s[aComp], b);}
    public void xor(int comp, long[] o, long[] a, @Nonnull Bitset b) {xor(o, s[comp], s[comp+1], a, s[comp], b);}
    public void xor(long[] a, @Nonnull Bitset b) {xor(a, 0, a.length, a, 0, b);}
    public void xor(long[] a, int aComp, @Nonnull Bitset b) {xor(a, s[aComp], s[aComp+1], a, s[aComp], b);}

    public boolean containsAll(long[] a, long[] b) { return containsAll(a, 0, a.length, b, 0); }
    public boolean containsAll(int comp, long[] a, long[] b) { return containsAll(a, s[comp], s[comp+1], b, s[comp]); }
    public boolean containsAll(long[] a, int comp, long[] b) { return containsAll(a, s[comp], s[comp+1], b, 0); }
    public boolean containsAll(long[] a, long[] b, int comp) { return containsAll(a, 0, a.length, b, s[comp]); }
    public boolean containsAll(@Nonnull Bitset a, long[] b, int comp) { return containsAll(a, b, s[comp], s[comp+1]); }
    public boolean containsAll(long[] a, int comp, @Nonnull Bitset b) { return containsAll(a, s[comp], s[comp+1], b); }

    public boolean equals(long[] a, long[] b) { return equals(a, 0, a.length, b, 0); }
    public boolean equals(int comp, long[] a, long[] b) { return equals(a, s[comp], s[comp+1], b, s[comp]); }
    public boolean equals(long[] a, int comp, long[] b) { return equals(a, s[comp], s[comp+1], b, 0); }
    public boolean equals(long[] a, long[] b, int comp) { return equals(a, 0, a.length, b, s[comp]); }

    public static boolean equals(long[] a, @Nonnull Bitset b) { return equals(a, 0, a.length, b);}
    public boolean equals(long[] a, int comp, @Nonnull Bitset b) { return equals(a, s[comp], s[comp+1], b);}

    public static boolean and(long[] o, int oBegin, int oEnd, long[] a, int aBegin, long[] b, int bBegin) {
        assert a.length-aBegin >= oEnd-oBegin; // a range must be >= o range, but b can be smaller
        int aAdjust = aBegin - oBegin, bAdjust = bBegin - oBegin;
        int oEnd1 = Math.min(oEnd, oBegin+b.length-bBegin);
        boolean notEmpty = false;
        for (int i = oBegin; i < oEnd1; i++)
            notEmpty |= (o[i] = a[i+ aAdjust] & b[i + bAdjust]) != 0;
        Arrays.fill(o, oEnd1, oEnd, 0); // a[i+aAdjust] & 0 == 0
        return notEmpty;
    }

    public static void and(long[] o, int oBegin, int oEnd, long[] a, int aBegin,
                           @Nonnull Bitset b) {
        int aAdjust = aBegin - oBegin;
        assert oEnd + aAdjust <= a.length;
        for (int i = oBegin; i < oEnd; i++)
            o[i] = a[i + aAdjust] & b.word(i-oBegin);
    }

    public static boolean intersects(long[] a, int aBegin, int aEnd, long[] b, int bBegin) {
        int adjust = bBegin - aBegin;
        aEnd = Math.min(aEnd, aBegin+(b.length-bBegin));
        for (int i = aBegin; i < aEnd; i++) {
            if ((a[i] & b[i + adjust]) != 0) return true;
        }
        return false;
    }

    public static boolean or(long[] o, int oBegin, int oEnd, long[] a, int aBegin, long[] b, int bBegin) {
        assert a.length-aBegin >= oEnd-oBegin; // a range must be >= o range, but b can be smaller
        int aAdjust = aBegin - oBegin, bAdjust = bBegin - oBegin;
        int oEnd1 = Math.min(oEnd, oBegin+b.length-bBegin);
        boolean notEmpty = false;
        for (int i = oBegin; i < oEnd1; i++)
            notEmpty |= (o[i] = a[i+ aAdjust] | b[i + bAdjust]) != 0;
        for (int i = oEnd1; i < oEnd; i++)
            notEmpty |= (o[i] = a[i+aAdjust]) != 0;
        return notEmpty;
    }

    public static void or(long[] o, int oBegin, int oEnd, long[] a, int aBegin, @Nonnull Bitset b) {
        int aAdjust = aBegin - oBegin;
        assert oEnd + aAdjust <= a.length;
        for (int i = oBegin; i < oEnd; i++)
            o[i] = a[i + aAdjust] | b.word(i-oBegin);
    }

    public static boolean xor(long[] o, int oBegin, int oEnd, long[] a, int aBegin,
                              long[] b, int bBegin) {
        assert a.length-aBegin >= oEnd-oBegin; // a range must be >= o range, but b can be smaller
        int aAdjust = aBegin - oBegin, bAdjust = bBegin - oBegin;
        int oEnd1 = Math.min(oEnd, oBegin+b.length-bBegin);
        boolean notEmpty = false;
        for (int i = oBegin; i < oEnd1; i++)
            notEmpty |= (o[i] = a[i+ aAdjust] ^ b[i + bAdjust]) != 0;
        for (int i = oEnd1; i < oEnd; i++)
            notEmpty |= (o[i] = a[i+aAdjust]) != 0;
        return notEmpty;
    }

    public static void xor(long[] o, int oBegin, int oEnd, long[] a, int aBegin, @Nonnull Bitset b) {
        int aAdjust = aBegin - oBegin;
        assert oEnd + aAdjust <= a.length;
        for (int i = oBegin; i < oEnd; i++)
            o[i] = a[i + aAdjust] ^ b.word(i-oBegin);
    }

    public static boolean andNot(long[] o, int oBegin, int oEnd, long[] a, int aBegin, long[] b, int bBegin) {
        assert a.length-aBegin >= oEnd-oBegin; // a range must be >= o range, but b can be smaller
        int aAdjust = aBegin - oBegin, bAdjust = bBegin - oBegin;
        int oEnd1 = Math.min(oEnd, oBegin+b.length-bBegin);
        boolean notEmpty = false;
        for (int i = oBegin; i < oEnd1; i++)
            notEmpty |= (o[i] = a[i+ aAdjust] & ~b[i + bAdjust]) != 0;
        for (int i = oEnd1; i < oEnd; i++)
            notEmpty |= (o[i] = a[i+aAdjust]) != 0;
        return notEmpty;
    }

    public static void andNot(long[] o, int oBegin, int oEnd, long[] a, int aBegin, @Nonnull Bitset b) {
        int aAdjust = aBegin - oBegin;
        assert oEnd + aAdjust <= a.length;
        for (int i = oBegin; i < oEnd; i++)
            o[i] = a[i + aAdjust] &~ b.word(i-oBegin);
    }

    public boolean containsAll(long[] a, int aBegin, int aEnd, long[] b, int bBegin) {
        int adjust = bBegin - aBegin;
        for (int i = aBegin; i < aEnd; i++) {
            if ((b[i + adjust] & ~a[i]) != 0)
                return false; //there is a bit in b that is not in a
        }
        return true;
    }

    public static boolean containsAll(@Nonnull Bitset a, long[] b, int bBegin, int bEnd) {
        for (int i = 0, words = bEnd-bBegin; i < words; i++) {
            if ((b[bBegin+i] & ~a.word(i)) != 0)
                return false;
        }
        return true;
    }

    public static boolean containsAll(long[] a, int aBegin, int aEnd, @Nonnull Bitset b) {
        for (int i = 0, words = aEnd-aBegin; i < words; i++) {
            if ((b.word(i) & ~a[aBegin+i]) != 0)
                return false;
        }
        return true;
    }

    public boolean equals(long[] a, int aBegin, int aEnd, long[] b, int bBegin) {
        int adjust = bBegin - aBegin;
        if (b.length-bBegin < aEnd-aBegin)
            return false; // different sizes
        for (int i = aBegin; i < aEnd; i++) {
            if (a[i] != b[i + adjust]) return false;
        }
        return true;
    }

    public static boolean equals(long[] a, int aBegin, int aEnd, @Nonnull Bitset b) {
        for (int i = aBegin; i < aEnd; i++) {
            if (a[i] != b.word(i-aBegin)) return false;
        }
        return true;
    }

    public void toggle(long[] a, int comp, int bit) {
        a[s[comp] + wordIndex(bit)] ^= (1L << bit);
    }
    public void flip(long[] a, int comp) { flip(a, s[comp], s[comp+1]); }
    public static void flip(long[] a) { flip(a, 0, a.length); }

    public static void flip(long[] a, int aBegin, int aEnd) {
        for (int i = aBegin; i < aEnd; i++)
            a[i] = ~a[i];
    }
    public int cardinality(long[] data, int component) {
        return cardinality(data, s[component], s[component+1]);
    }
    public static int cardinality(long[] data) {
        return cardinality(data, 0, data.length);
    }

    public static int cardinality(long[] data, int beginWord, int endWord) {
        int sum = 0;
        for (int i = beginWord; i < endWord; i++) sum += Long.bitCount(data[i]);
        return sum;
    }
    public int nextSet(long[] data, int component, int index) {
        return nextSet(data, s[component], s[component+1], index);
    }
    public static int nextSet(long[] data, int index) {
        return nextSet(data, 0, data.length, index);
    }

    private static int nextSet(long[] data, int beginWord, int endWord, int beginBit) {
        int wordIndex = beginWord + wordIndex(beginBit);
        assert wordIndex <= endWord;
        if (wordIndex >= endWord)
            return -1; //no bits in component
        long word = data[wordIndex] & (ALL_ON << beginBit);
        int i = wordIndex;
        while (true){
            if (word != 0)
                return (i-beginWord)*64 + Long.numberOfTrailingZeros(word);
            if (++i == endWord)
                return -1;
            word = data[i];
        }
    }
    public int nextClear(long[] data, int component, int bitIndex) {
        return nextClear(data, s[component], s[component+1], bitIndex);
    }

    public static int nextClear(long[] data, int bitIndex) {
        return nextClear(data, 0, data.length, bitIndex);
    }

    private static int nextClear(long[] data, int beginWord, int endWord, int beginBit) {
        int wordIndex = beginWord + wordIndex(beginBit);
        long word = ~data[wordIndex] & (ALL_ON << beginBit);
        int i = wordIndex;
        while (true){
            if (word != 0)
                return (i-beginWord)*64 + Long.numberOfTrailingZeros(word);
            if (++i == endWord)
                return -1;
            word = ~data[i];
        }
    }

    public @Nonnull Bitset asBitset(long[] data, int component) {
        return new SegmentBitset(data, s[component], s[component+1]);
    }

    public static @Nonnull Bitset asBitset(long[] data) {
        return new SegmentBitset(data, 0, data.length);
    }
}
