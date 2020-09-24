package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class RawAlignedBitSetTest {

    @DataProvider public @Nonnull Object[][] testAllocData() {
        return Stream.of(
                asList(0, 0),
                asList(1, 1),
                asList(32, 1),
                asList(63, 1),
                asList(64, 1),
                asList(65, 2),
                asList(96, 2),
                asList(127, 2),
                asList(128, 2),
                asList(129, 3)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testAllocData")
    public void testAlloc(int bits, int expectedWords) {
        assertEquals(new RawAlignedBitSet(bits).alloc().length, expectedWords);
    }

    @DataProvider public @Nonnull Object[][] testAllocSequenceData() {
        return Stream.of(
                asList(new int[]{0}, 0),
                asList(new int[]{1}, 1),
                asList(new int[]{63}, 1),
                asList(new int[]{64}, 1),
                asList(new int[]{65}, 2),
                asList(new int[]{1, 1}, 2),
                asList(new int[]{1, 64}, 2),
                asList(new int[]{0, 64}, 1),
                asList(new int[]{1, 64, 128, 192}, 1+1+2+3),
                asList(new int[]{1, 64, 128, 193}, 1+1+2+4)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testAllocSequenceData")
    public void testAllocSequence(@Nonnull int[] sizes, int expectedWords) {
        assertEquals(new RawAlignedBitSet(sizes).alloc().length, expectedWords);
    }

    @DataProvider public @Nonnull Object[][] singleBitTestData() {
        return Stream.of(
                singletonList(new int[]{1}),
                singletonList(new int[]{64}),
                singletonList(new int[]{64, 32}),
                singletonList(new int[]{64, 128, 64}),
                singletonList(new int[]{64, 128, 1, 128, 192, 1})
        ).map(List::toArray).toArray(Object[][]::new);
    }

    private void assertSingleBit(RawAlignedBitSet bs, long[] data, int component,
                                 int bit, boolean expected, String context) {
        for (int i = 0, nComponents = bs.getComponentCount(); i < nComponents; i++) {
            for (int j = 0, nBits = bs.getBitCount(i); j < nBits; j++) {
                boolean bitExpected = i == component && j == bit ? expected : !expected;
                String msg = format("i=%d, j=%d, context: %s", i, j, context);
                assertEquals(bs.get(data, i, j), bitExpected, msg);
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testSetGetUnsetToggleTwice(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] data = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                bs.set(data, i, j);
                assertSingleBit(bs, data, i, j, true, format("i=%d, j=%d", i, j));
                bs.unset(data, i, j);
                assertTrue(RawAlignedBitSet.isEmpty(data));
                assertTrue(Arrays.stream(data).allMatch(Long.valueOf(0)::equals));
            }
        }
        assertTrue(RawAlignedBitSet.isEmpty(data));
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++)
            assertTrue(bs.isEmpty(data, i), "i="+i);
    }

    @Test(dataProvider = "singleBitTestData")
    public void testToggleTwice(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] data = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                bs.toggle(data, i, j);
                assertSingleBit(bs, data, i, j, true, format("i=%d, j=%d", i, j));
                bs.toggle(data, i, j);
                assertTrue(bs.isEmpty(data, i));
                assertTrue(RawAlignedBitSet.isEmpty(data));
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testToggleFlipToggle(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] data = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                bs.toggle(data, i, j);
                assertSingleBit(bs, data, i, j, true, format("i=%d, j=%d", i, j));
                RawAlignedBitSet.flip(data);
                assertSingleBit(bs, data, i, j, false, format("i=%d, j=%d", i, j));
                RawAlignedBitSet.flip(data, 0, data.length);
                assertSingleBit(bs, data, i, j, true, format("i=%d, j=%d", i, j));
                for (int k = 0; k < nComponents; k++)
                    bs.flip(data, k);
                assertSingleBit(bs, data, i, j, false, format("i=%d, j=%d", i, j));
                bs.toggle(data, i, j); // all 1's
                RawAlignedBitSet.flip(data, 0, data.length); // all 0's
                assertTrue(RawAlignedBitSet.isEmpty(data));
                assertTrue(bs.isEmpty(data, i));
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testCardinalityFillUpwards(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] data = bs.alloc();
        int expected = 0;
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                bs.set(data, i, j);
                ++expected;
                assertEquals(bs.cardinality(data, i), j+1);
                assertEquals(RawAlignedBitSet.cardinality(data), expected);
            }
        }
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                bs.unset(data, i, j);
                --expected;
                assertEquals(bs.cardinality(data, i), componentSizes[i] - j - 1);
                assertEquals(RawAlignedBitSet.cardinality(data), expected);
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testNextSetBit(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] data = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                bs.set(data, i, j);
                assertEquals(bs.nextSet(data, i, 0), j);
                assertEquals(bs.nextSet(data, i, j), j);
                if (j+1 < componentSizes[i])
                    assertEquals(bs.nextSet(data, i, j+1), -1);
                if (i+1 < nComponents) { // do not return bits from next component
                    bs.set(data, i+1, 0);
                    bs.set(data, i+1, Math.min(componentSizes[i+1]-1, j));
                    if (j+1 < componentSizes[i])
                        assertEquals(bs.nextSet(data, i, j + 1), -1);
                    bs.unset(data, i+1, 0);
                    bs.unset(data, i+1, Math.min(componentSizes[i+1]-1, j));
                }
                bs.unset(data, i, j);
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testNextClearBit(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] data = bs.alloc();
        RawAlignedBitSet.flip(data); // set all bits
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            for (int j = 0; j < componentSizes[i]; j++) {
                String msg = format("i=%d, j=%d", i, j);
                bs.unset(data, i, j);
                assertEquals(bs.nextClear(data, i, 0), j, msg);
                assertEquals(bs.nextClear(data, i, j), j, msg);
                if (j+1 < componentSizes[i])
                    assertEquals(bs.nextClear(data, i, j+1), -1);
                if (i+1 < componentSizes.length) { // do not return bits from next component
                    bs.unset(data, i+1, 0);
                    bs.unset(data, i+1, Math.min(componentSizes[i+1]-1, j));
                    if (j+1 < componentSizes[i])
                        assertEquals(bs.nextClear(data, i, j + 1), -1, msg);
                    bs.set(data, i+1, 0);
                    bs.set(data, i+1, Math.min(componentSizes[i+1]-1, j));
                }
                bs.set(data, i, j);
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testEqualsContainsAll(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] a = bs.alloc(), b = bs.alloc();
        for (int i = 0; i < componentSizes.length; i++) {
            String msg = "i="+i;
            bs.flip(a, i);
            bs.flip(b, i);
            assertTrue(bs.containsAll(i, a, b), msg);
            assertTrue(bs.containsAll(i, b, a), msg);
            assertTrue(bs.equals(i, b, a), msg);
        }
        for (int i = 0; i < componentSizes.length; i++) {
            bs.flip(a, i);
            bs.flip(b, i);
            assertTrue(bs.containsAll(i, a, b));
            assertTrue(bs.containsAll(i, b, a));
            assertTrue(bs.equals(i, b, a));
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testIntersect(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] a = bs.alloc(), b = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            assertFalse(bs.intersects(i, a, b));
            int size = componentSizes[i];
            bs.set(a, i, 0);
            bs.set(b, i, size -1);
            assertEquals(bs.intersects(i, a, b), size == 1);
            int mid = size / 2;
            bs.set(a, i, mid);
            bs.set(b, i, mid);
            assertTrue(bs.intersects(i, a, b));
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testCopyAndAllBits(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] a = bs.alloc(), b = bs.alloc(), c = bs.alloc(), ex = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            int size = componentSizes[i];
            for (int j = 0; j < size; j++) {
                bs.set(a, i, j);
                long[] aCopy = Arrays.copyOf(a, a.length);
                boolean notEmpty = bs.and(i, c, a, b);
                assertEquals(notEmpty, !bs.isEmpty(ex, i));
                assertEquals(a, aCopy);
                assertEquals(c, ex);

                bs.set(b, i, j);
                bs.set(ex, i, j);
                assertEquals(a, b);

                notEmpty = bs.and(i, c, a, b);
                assertEquals(notEmpty, !bs.isEmpty(ex, i));
                assertEquals(a, aCopy);
                assertEquals(c, ex);
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testCopyAndAllBitsComponent(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] a = bs.alloc(), b = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            int size = componentSizes[i];
            long[] c = new long[size], ex = new long[size];
            for (int j = 0; j < size; j++) {
                bs.set(a, i, j);
                bs.set(b, i, j);
                RawAlignedBitSet.set(ex, j);
                assertEquals(a, b);

                long[] aCopy = Arrays.copyOf(a, a.length);
                Arrays.fill(c, 0);
                boolean notEmpty = bs.and(c, a, i, b, i);
                assertEquals(notEmpty, !RawAlignedBitSet.isEmpty(ex));
                assertEquals(a, aCopy);
                assertEquals(c, ex);
            }
        }
    }

    @Test(dataProvider = "singleBitTestData")
    public void testXor(int[] componentSizes) {
        RawAlignedBitSet bs = new RawAlignedBitSet(componentSizes);
        long[] a = bs.alloc(), b = bs.alloc(), c = bs.alloc();
        for (int i = 0, nComponents = componentSizes.length; i < nComponents; i++) {
            int size = componentSizes[i];
            for (int j = 0; j < size; j++) {
                RawAlignedBitSet.clear(c);
                assertFalse(bs.xor(i, c, a, b));

                bs.set(a, i, j); //causes a single bit diff
                RawAlignedBitSet.clear(c);
                assertTrue(bs.xor(i, c, a, b));
                assertEquals(bs.cardinality(c, i), 1);
                assertTrue(bs.get(c, i, j));

                bs.set(b, i, j); // a and b now equal
                assertEquals(a, b);
                RawAlignedBitSet.clear(c);
                assertFalse(bs.xor(c, a, b));
                assertTrue(RawAlignedBitSet.isEmpty(c));
            }
        }
    }
}