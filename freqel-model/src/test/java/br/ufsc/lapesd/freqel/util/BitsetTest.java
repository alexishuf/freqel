package br.ufsc.lapesd.freqel.util;

import br.ufsc.lapesd.freqel.util.bitset.*;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class BitsetTest {

    private interface Factory {
        int sizeLimit();
        @Nonnull Bitset fromJava(@Nonnull BitSet java, int bitsSize);
    }

    private final List<Factory> factories = asList(
            new Factory() {
                @Override public String toString() { return "LongBitset"; }
                @Override public int sizeLimit() { return 64; }
                @Override public @Nonnull Bitset fromJava(@Nonnull BitSet java, int ignored) {
                    return LongBitset.fromJava(java);
                }
            },
            new Factory() {
                @Override public String toString() { return "BitSetDelegate"; }
                @Override public int sizeLimit() { return Integer.MAX_VALUE; }
                @Override public @Nonnull Bitset fromJava(@Nonnull BitSet java, int ignored) {
                    return new BitSetDelegate((BitSet) java.clone());
                }
            },
            new Factory() {
                @Override public String toString() { return "ArrayBitset"; }
                @Override public int sizeLimit() { return Integer.MAX_VALUE; }
                @Override public @Nonnull Bitset fromJava(@Nonnull BitSet java, int ignored) {
                    return ArrayBitset.fromJava(java);
                }
            },
            new Factory() {
                @Override public String toString() { return "SegmentBitset"; }
                @Override public int sizeLimit() { return Integer.MAX_VALUE; }
                @Override public @Nonnull Bitset fromJava(@Nonnull BitSet java, int bitsSize) {
                    long[] data = new long[(bitsSize >> 6) + 4];
                    long[] src = java.toLongArray();
                    System.arraycopy(src, 0, data, 2, src.length);
                    return new SegmentBitset(data, 2, data.length - 1);
                }
            },
            new Factory() {
                @Override public String toString() { return "DynamicBitset"; }
                @Override public int sizeLimit() { return Integer.MAX_VALUE; }
                @Override public @Nonnull Bitset fromJava(@Nonnull BitSet java, int bitsSize) {
                    return DynamicBitset.fromJava(java);
                }
            }
    );

    private void checkEquals(@Nonnull Bitset actual, @Nonnull BitSet expected) {
        Assert.assertEquals(actual.length(), expected.length());
        Assert.assertEquals(actual.cardinality(), expected.cardinality());
        for (int i = 0, end = Math.max(actual.size(), expected.size()); i < end; i++)
            Assert.assertEquals(actual.get(i), expected.get(i), "i="+i);

    }

    private void checkEquals(@Nonnull Bitset actual, @Nonnull BitSet expected,
                             @Nonnull String context) {
        String acStr = actual.toString(), exStr = expected.toString();
        for (int i = 0, end = Math.max(actual.size(), expected.size()); i < end; i++) {
            Assert.assertEquals(actual.get(i), expected.get(i), "i="+i+", context: "+context+
                    "\nactual  :"+acStr+
                    "\nexpected:"+exStr);
        }
        Assert.assertEquals(actual.length(), expected.length(), context);
        Assert.assertEquals(actual.cardinality(), expected.cardinality(), context);
    }

    private void runGetSetFlip(@Nonnull Bitset actual, @Nonnull BitSet expected, int size) {
        checkEquals(actual, expected);

        boolean old = actual.get(0);
        actual.set(0, !old);
        Assert.assertEquals(actual.get(0), !old);
        expected.set(0, !old);
        checkEquals(actual, expected);

        old = actual.get(size-1);
        actual.set(size-1, !old);
        Assert.assertEquals(actual.get(size-1), !old);
        expected.set(size-1, !old);
        checkEquals(actual, expected);

        old = actual.get(size/2);
        actual.set(size/2, !old);
        Assert.assertEquals(actual.get(size/2), !old);
        expected.set(size/2, !old);
        checkEquals(actual, expected);

        actual.flip(0);
        expected.flip(0);
        checkEquals(actual, expected);

        actual.flip(size-1);
        expected.flip(size-1);
        checkEquals(actual, expected);

        actual.flip(size/2);
        expected.flip(size/2);
        checkEquals(actual, expected);

        assert size / 2 > 0 && size - 1 > 0;
        boolean oldBit = actual.get(0);
        Assert.assertEquals(actual.compareAndSet(0), !oldBit);
        expected.set(0);
        checkEquals(actual, expected);

        assertFalse(actual.compareAndSet(0));
        expected.set(0);
        checkEquals(actual, expected);
    }

    @DataProvider
    public @Nonnull Object[][] factoriesAndSizeData() {
        return Stream.of(4, 32, 64, 96, 128, 192)
                .flatMap(i -> factories.stream().filter(f -> f.sizeLimit() >= i)
                                                .map(f -> new Object[] {f, i}))
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testGetSetFlip(Factory f, int size) {
        BitSet java = new BitSet();
        runGetSetFlip(f.fromJava(java, size), java, size);
        java.clear();
        java.flip(0, size);
        runGetSetFlip(f.fromJava(java, size), java, size);
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testRangeSet(Factory f, int size) {
        for (int i = 0; i < size; i++) {
            BitSet expected = new BitSet();
            Bitset actual = f.fromJava(expected, size);
            expected.set(0, i+1);
            Assert.assertEquals(actual.cardinality(), 0);
            actual.set(0, i+1);
            checkEquals(actual, expected);
        }
        for (int i = 1; i <= size; i++) {
            BitSet expected = new BitSet();
            Bitset actual = f.fromJava(expected, size);
            expected.set(size-i, size);
            Assert.assertEquals(actual.cardinality(), 0);
            actual.set(size-i, size);
            checkEquals(actual, expected);
        }
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testRangeFlip(Factory f, int size) {
        for (int i = 0; i < size; i++) {
            BitSet expected = new BitSet();
            Bitset actual = f.fromJava(expected, size);
            expected.flip(0, i+1);
            Assert.assertEquals(actual.cardinality(), 0);
            actual.flip(0, i+1);
            checkEquals(actual, expected);

            expected.flip(0, i+1);
            actual.flip(0, i+1);
            checkEquals(actual, expected);
        }
        for (int i = 1; i <= size; i++) {
            BitSet expected = new BitSet();
            Bitset actual = f.fromJava(expected, size);
            expected.flip(size-i, size);
            Assert.assertEquals(actual.cardinality(), 0);
            actual.flip(size-i, size);
            checkEquals(actual, expected);

            expected.flip(size-i, size);
            actual.flip(size-i, size);
            checkEquals(actual, expected);
        }
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testHashCode(Factory f, int size) {
        for (int i = 0; i < size; i++) {
            BitSet java = new BitSet();
            java.set(0, i+1);
            Bitset ac1 = f.fromJava(java, size), ac2 = f.fromJava(new BitSet(), size);
            int dummy = ac2.hashCode(); //hit inner cache, if any
            if (ac2.equals(ac1))
                Assert.assertEquals(dummy, ac1.hashCode());
            ac2.set(0, i+1);
            Assert.assertEquals(ac1.hashCode(), java.hashCode());
            Assert.assertEquals(ac2.hashCode(), java.hashCode());
        }
        for (int i = 1; i <= size; i++) {
            BitSet java = new BitSet();
            java.set(size-i, size);
            Bitset ac1 = f.fromJava(java, size), ac2 = f.fromJava(new BitSet(), size);
            int dummy = ac2.hashCode(); //hit inner cache, if any
            if (ac2.equals(ac1))
                Assert.assertEquals(dummy, ac1.hashCode());
            ac2.set(size-i, size);
            Assert.assertEquals(ac1.hashCode(), java.hashCode());
            Assert.assertEquals(ac2.hashCode(), java.hashCode());
        }
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testEquals(Factory f, int size) {
        for (int i = 0; i < size; i++) {
            BitSet java = new BitSet();
            java.set(0, i+1);
            Bitset ac1 = f.fromJava(java, size), ac2 = f.fromJava(new BitSet(), size);
            ac2.set(0, i+1);
            Assert.assertEquals(ac1, ac2, "i="+i);
        }
        for (int i = 1; i <= size; i++) {
            BitSet java = new BitSet();
            java.set(size-i, size);
            Bitset ac1 = f.fromJava(java, size), ac2 = f.fromJava(new BitSet(), size);
            ac2.set(size-i, size);
            Assert.assertEquals(ac1, ac2);
        }
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testRandomBitsEquals(Factory f, int size) {
        Random random = new Random(978124);
        BitSet java = new BitSet();
        Bitset a1 = f.fromJava(java, size);
        for (int i = 0; i < size; i++) {
            if (random.nextBoolean()) {
                java.set(i);
                a1.set(i);
            }
        }
        Bitset a2 = f.fromJava(java, size);
        Assert.assertEquals(a1.hashCode(), a2.hashCode());
        Assert.assertEquals(a1, a2);
        Assert.assertEquals(a1.toString(), a2.toString());
    }

    @DataProvider public @Nonnull Object[][] binaryMutationData() {
        Stream<ImmutablePair<BiConsumer<Bitset, Bitset>,BiConsumer<BitSet, BitSet>>> consumers =
                Stream.of(
                        ImmutablePair.of(Bitset::and,    BitSet::and),
                        ImmutablePair.of(Bitset::or,     BitSet::or),
                        ImmutablePair.of(Bitset::xor,    BitSet::xor),
                        ImmutablePair.of(Bitset::andNot, BitSet::andNot));
        return consumers.flatMap(p -> {
            List<Object[]> copies = new ArrayList<>();
            for (Object[] row : factoriesAndSizeData()) {
                Object[] copy = Arrays.copyOf(row, row.length + 1);
                copy[row.length] = p;
                copies.add(copy);
            }
            return copies.stream();
        }).toArray(Object[][]::new);
    }

    @Test(dataProvider = "binaryMutationData")
    public void testBinaryMutation(Factory f, int size,
                                   ImmutablePair<BiConsumer<Bitset, Bitset>,
                                                 BiConsumer<BitSet, BitSet>> mutators) {
        BitSet jLeft = new BitSet(), jRight = new BitSet();
        // left:  1 .... 1 .... 0
        // right: 1 .... 0 .... 1
        jLeft.set(0);
        jLeft.set(size/2);
        jRight.set(0);
        jRight.set(size-1);

        Bitset aLeft = f.fromJava(jLeft, size), aRight = f.fromJava(jRight, size);
        mutators.left.accept(aLeft, aRight);
        mutators.right.accept(jLeft, jRight);
        checkEquals(aLeft, jLeft);
        checkEquals(aRight, jRight);
    }

    @DataProvider public @Nonnull Object[][] binaryCreationData() {
        Stream<ImmutablePair<BiFunction<Bitset, Bitset, Bitset>,BiConsumer<BitSet, BitSet>>>
                consumers = Stream.of(
                        ImmutablePair.of(Bitset::createAnd,    BitSet::and),
                        ImmutablePair.of(Bitset::createOr,     BitSet::or),
                        ImmutablePair.of(Bitset::createXor,    BitSet::xor),
                        ImmutablePair.of(Bitset::createAndNot, BitSet::andNot));
        return consumers.flatMap(p -> {
            List<Object[]> copies = new ArrayList<>();
            for (Object[] row : factoriesAndSizeData()) {
                Object[] copy = Arrays.copyOf(row, row.length + 1);
                copy[row.length] = p;
                copies.add(copy);
            }
            return copies.stream();
        }).toArray(Object[][]::new);
    }

    @Test(dataProvider = "binaryCreationData")
    public void testBinaryCreation(Factory f, int size,
                                   ImmutablePair<BiFunction<Bitset, Bitset, Bitset>,
                                                 BiConsumer<BitSet, BitSet>> mutators) {
        BitSet jLeft = new BitSet(), jRight = new BitSet();
        // left:  1 .... 1 .... 0
        // right: 1 .... 0 .... 1
        jLeft.set(0);
        jLeft.set(size/2);
        jRight.set(0);
        jRight.set(size-1);

        BitSet expected = (BitSet) jLeft.clone();
        mutators.right.accept(expected, jRight);

        Bitset aLeft = f.fromJava(jLeft, size), aRight = f.fromJava(jRight, size);
        Bitset actual = mutators.left.apply(aLeft, aRight);

        checkEquals(actual, expected);
        checkEquals(aLeft, jLeft); // no change
        checkEquals(aRight, jRight); // no change
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testSegmentCardinalities(Factory f, int size) {
        BitSet java = new BitSet();
        java.set(0, size);

        Bitset bitset = f.fromJava(java, size);
        Assert.assertEquals(bitset.cardinality(), size);
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(bitset.cardinalityBefore(i), i);
            Assert.assertEquals(bitset.cardinalityFrom(i), size-i);
            bitset.clear(i);
            Assert.assertEquals(bitset.cardinalityFrom(i), size-i-1);
            bitset.set(i);
            Assert.assertEquals(bitset.cardinalityAfter(i), size-i-1);
        }
    }

    @Test(dataProvider = "factoriesAndSizeData")
    public void testIterateAllBits(Factory f, int size) {
        BitSet java = new BitSet();
        java.flip(0, size);
        Bitset a = f.fromJava(java, size);

        Assert.assertEquals(a.previousClearBit(size), size);
        Assert.assertEquals(a.nextClearBit(size), size);
        Assert.assertEquals(a.previousClearBit(size+1), size+1);
        Assert.assertEquals(a.nextClearBit(size+1), size+1);
        Assert.assertEquals(a.previousClearBit(size*2), size*2);
        Assert.assertEquals(a.nextClearBit(size*2), size*2);

        Assert.assertEquals(a.nextSetBit(size), -1);
        Assert.assertEquals(a.previousSetBit(size), size-1);
        Assert.assertEquals(a.nextSetBit(size+1), -1);
        Assert.assertEquals(a.previousSetBit(size+1), size-1);
        Assert.assertEquals(a.nextSetBit(size*2), -1);
        Assert.assertEquals(a.previousSetBit(size*2), size-1);

        List<Integer> expected = IntStream.range(0, size).boxed().collect(toList());
        List<Integer> indices = new ArrayList<>();
        for (int i = a.nextSetBit(0); i >= 0; i = a.nextSetBit(i+1))
            indices.add(i);
        assertEquals(indices, expected);

        indices.clear();
        expected = Lists.reverse(expected);
        for (int i = a.previousSetBit(a.length()); i >= 0; i = a.previousSetBit(i-1))
            indices.add(i);
        assertEquals(indices, expected);

        java.flip(0, size);
        a.flip(0, size);
        checkEquals(a, java);

        Assert.assertEquals(a.previousClearBit(size), size);
        Assert.assertEquals(a.nextClearBit(size), size);
        Assert.assertEquals(a.previousClearBit(size+1), size+1);
        Assert.assertEquals(a.nextClearBit(size+1), size+1);
        Assert.assertEquals(a.previousClearBit(size*2), size*2);
        Assert.assertEquals(a.nextClearBit(size*2), size*2);

        Assert.assertEquals(a.nextSetBit(size), -1);
        Assert.assertEquals(a.nextSetBit(size+1), -1);
        Assert.assertEquals(a.nextSetBit(size*2), -1);
        Assert.assertEquals(a.previousSetBit(size), -1);
        Assert.assertEquals(a.previousSetBit(size+1), -1);
        Assert.assertEquals(a.previousSetBit(size*2), -1);

        indices.clear();
        expected = Lists.reverse(expected);
        for (int i = a.nextClearBit(0); i < size; i = a.nextClearBit(i+1))
            indices.add(i);
        assertEquals(indices, expected);

        indices.clear();
        expected = Lists.reverse(expected);
        for (int i = a.previousClearBit(size-1); i >= 0; i = a.previousClearBit(i-1))
            indices.add(i);
        assertEquals(indices, expected);

        Assert.assertEquals(a.previousClearBit(size), size);
        Assert.assertEquals(a.nextClearBit(size), size);
    }

    interface FragmentBinaryMutator {
        void mutate(@Nonnull Bitset l, @Nonnull Bitset r, int lStart, int rStart, int bits);
    }

    @DataProvider public @Nonnull Object[][] fragmentBinaryMutationData() {
        return Stream.of(
                asList((FragmentBinaryMutator) (l, r, lStart, rStart, bits) ->
                                l.and(lStart, r, rStart, bits),
                        (BiFunction<Boolean, Boolean, Boolean>)(l, r) -> l && r),
                asList((FragmentBinaryMutator) (l, r, lStart, rStart, bits) ->
                                l.assign(lStart, r, rStart, bits),
                        (BiFunction<Boolean, Boolean, Boolean>)(l, r) -> r),
                asList((FragmentBinaryMutator) (l, r, lStart, rStart, bits) ->
                                l.or(lStart, r, rStart, bits),
                        (BiFunction<Boolean, Boolean, Boolean>)(l, r) -> l || r),
                asList((FragmentBinaryMutator) (l, r, lStart, rStart, bits) ->
                                l.xor(lStart, r, rStart, bits),
                        (BiFunction<Boolean, Boolean, Boolean>)(l, r) -> l ^ r),
                asList((FragmentBinaryMutator) (l, r, lStart, rStart, bits) ->
                                l.andNot(lStart, r, rStart, bits),
                        (BiFunction<Boolean, Boolean, Boolean>)(l, r) -> l && !r)
        ).flatMap(base -> Arrays.stream(factoriesAndSizeData()).map(array -> {
            Object[] row = Arrays.copyOf(array, array.length + 2);
            row[array.length  ] = base.get(0);
            row[array.length+1] = base.get(1);
            return row;
        })).toArray(Object[][]::new);
    }

    @Test(dataProvider = "fragmentBinaryMutationData")
    public void testFragmentBinaryMutation(Factory f, int size,
                                                  FragmentBinaryMutator mutator,
                                                  BiFunction<Boolean, Boolean, Boolean> booleanOp) {
        BitSet jLeft = new BitSet(), jRight = new BitSet();
        for (int i = 0; i < size+4; i += 4) {    // Generate this repeating pattern:
            if (i+2 < size) jLeft .set(i+2);     // 0 0 1 1 (left)
            if (i+3 < size) jLeft .set(i+3);
            if (i+1 < size) jRight.set(i+1);     // 0 1 0 1 (right)
            if (i+3 < size) jRight.set(i+3);
        }
        for (int lBegin = 0; lBegin < size - 1; lBegin++) {
            for (int lEnd : asList(lBegin+1, lBegin+2, lBegin+3, lBegin+4, size)) {
                if (lEnd > size)
                    continue;
                int bits = lEnd - lBegin;
                if (lBegin + bits > f.sizeLimit())
                    continue;
                for (int rBegin : asList(0, 1, size - bits)) {
                    if (rBegin + bits > f.sizeLimit())
                        continue;
                    Bitset left = f.fromJava(jLeft, size);
                    Bitset right = f.fromJava(jRight, size);
                    Bitset ac1 = left.copy(), ac2 = right.copy();
                    mutator.mutate(ac1, right, lBegin, rBegin, bits);
                    mutator.mutate(ac2, left, rBegin, lBegin, bits);

                    BitSet ex1 = (BitSet) jLeft.clone();
                    BitSet ex2 = (BitSet) jRight.clone();
                    for (int i = lBegin; i < lEnd; i++)
                        ex1.set(i, booleanOp.apply(jLeft.get(i), jRight.get(rBegin+i-lBegin)));
                    for (int i = rBegin; i < rBegin + bits; i++)
                        ex2.set(i, booleanOp.apply(jRight.get(i), jLeft.get(lBegin+i-rBegin)));

                    String context = String.format("lBegin=%d, lEnd=%d, rBegin=%d, size=%d",
                                                   lBegin, lEnd, rBegin, size);
                    checkEquals(ac1, ex1, context);
                    checkEquals(ac2, ex2, context);
                }
            }
        }
    }
}