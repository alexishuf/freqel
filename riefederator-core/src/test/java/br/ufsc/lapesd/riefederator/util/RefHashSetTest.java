package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class RefHashSetTest {

    private void check(@Nonnull Set<RefMapTest.Thing> actual,
                       @Nonnull Set<RefMapTest.Thing> expected) {
        checkEquality(actual, expected);
        checkIteration(actual, expected);
        checkAddIdempotency(actual, expected);
        checkNegativeContains(actual, expected);
    }

    private void checkNegativeContains(@Nonnull Set<RefMapTest.Thing> actual, @Nonnull Set<RefMapTest.Thing> expected) {
        for (RefMapTest.Thing thing : expected) {
            RefMapTest.Thing copy = new RefMapTest.Thing(thing.getId());
            assertFalse(actual.contains(copy));
        }
    }

    private void checkAddIdempotency(@Nonnull Set<RefMapTest.Thing> actual, @Nonnull Set<RefMapTest.Thing> expected) {
        for (RefMapTest.Thing thing : expected) {
            assertTrue(actual.contains(thing));
            assertFalse(actual.add(thing));
        }
        checkEquality(actual, expected);
    }

    private void checkIteration(@Nonnull Set<RefMapTest.Thing> actual, @Nonnull Set<RefMapTest.Thing> expected) {
        List<RefMapTest.Thing> observed = new ArrayList<>();
        for (Iterator<RefMapTest.Thing> it = actual.iterator(); it.hasNext(); )
            observed.add(it.next());
        assertEquals(observed.size(), expected.size());
        assertEquals(new HashSet<>(observed), expected);
    }

    private void checkEquality(@Nonnull Set<RefMapTest.Thing> actual, @Nonnull Set<RefMapTest.Thing> expected) {
        assertEquals(actual.isEmpty(), expected.isEmpty());
        assertEquals(actual.size(), expected.size());
        assertEquals(actual.iterator().hasNext(), expected.iterator().hasNext());
        assertTrue(expected.containsAll(actual));
        assertTrue(actual.containsAll(expected));
        //noinspection SimplifiableAssertion
        assertTrue(actual.equals(expected));
        //noinspection SimplifiableAssertion
        assertTrue(expected.equals(actual));
    }

    @DataProvider
    private static @Nonnull Object[][] sizesData() {
        return Stream.of(0, 1, 2, 3, 4, 8, 32, 64, 128)
                .map(i -> new Object[]{i}).toArray(Object[][]::new);
    }

    private void fill(@Nonnull Set<RefMapTest.Thing> actual,
                      @Nonnull Set<RefMapTest.Thing> expected,
                      int from, int to, int step) {
        if (step < 0) {
            assert to <= from;
            for (int i = from; i > to; i += step) {
                RefMapTest.Thing thing = new RefMapTest.Thing(i);
                assertTrue(actual.add(thing));
                assertFalse(actual.add(thing));
                assertTrue(expected.add(thing));
            }
        } else {
            assert to >= from;
            for (int i = from; i < to; i += step) {
                RefMapTest.Thing thing = new RefMapTest.Thing(i);
                assertTrue(actual.add(thing));
                assertFalse(actual.add(thing));
                assertTrue(expected.add(thing));
            }
        }
        check(actual, expected);
    }

    @Test(dataProvider = "sizesData")
    public void testFill(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        fill(actual, new HashSet<>(), 0, size, 1);
    }

    @Test(dataProvider = "sizesData")
    public void testFillReserved(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>(size);
        fill(actual, new HashSet<>(), 0, size, 1);
    }

    @Test(dataProvider = "sizesData")
    public void testFillUnderReserved(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>(size/2);
        fill(actual, new HashSet<>(), 0, size, 1);
    }

    @Test(dataProvider = "sizesData")
    public void testFillEven(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        fill(actual, new HashSet<>(), 0, size, 2);
    }

    @Test(dataProvider = "sizesData")
    public void testFillBackwards(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        fill(actual, new HashSet<>(), size, -1, -1);
    }

    @Test(dataProvider = "sizesData")
    public void testClear(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        fill(actual, new HashSet<>(), size, -1, -1);
        actual.clear();
        check(actual, emptySet());
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAll(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        HashSet<RefMapTest.Thing> expected = new HashSet<>();
        fill(actual, expected, size, -1, -1);
        assertTrue(actual.removeAll(expected));
        check(actual, emptySet());
        assertFalse(actual.removeAll(expected));
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAllManual(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        HashSet<RefMapTest.Thing> expected = new HashSet<>();
        fill(actual, expected, size, -1, -1);
        for (RefMapTest.Thing thing : expected)
            assertTrue(actual.remove(thing));
        check(actual, emptySet());

        for (RefMapTest.Thing thing : expected)
            assertFalse(actual.remove(thing));
        assertFalse(actual.removeAll(expected));
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveByIterator(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        HashSet<RefMapTest.Thing> expected = new HashSet<>();
        fill(actual, expected, size, -1, -1);
        Iterator<RefMapTest.Thing> it = actual.iterator();
        while (it.hasNext()) {
            RefMapTest.Thing thing = it.next();
            assertTrue(expected.contains(thing));
            it.remove();
            expected.remove(thing);
            check(actual, expected);
        }
        assertTrue(expected.isEmpty());
        check(actual, emptySet());
    }

    @Test(dataProvider = "sizesData")
    public void testStableHash(int size) {
        RefSet<RefMapTest.Thing> actual = new RefHashSet<>();
        HashSet<RefMapTest.Thing> expected = new HashSet<>();
        fill(actual, expected, 0, size, 1);
        RefSet<RefMapTest.Thing> actual2 = new RefHashSet<>(actual);
        HashSet<RefMapTest.Thing> expected2 = new HashSet<>(expected);

        assertEquals(expected2.hashCode(), expected.hashCode());
        assertEquals(actual2.hashCode(), actual.hashCode());
    }

    @Test(dataProvider = "sizesData")
    public void testConvertFromMap(int size) {
        RefHashMap<RefMapTest.Thing, Integer> map = new RefHashMap<>(size);
        HashSet<RefMapTest.Thing> expected = new HashSet<>();
        for (int i = 0; i < size; i++) {
            RefMapTest.Thing thing = new RefMapTest.Thing(i);
            assertNull(map.put(thing, i));
            expected.add(thing);
        }
        assertEquals(map.size(), size);
        RefHashSet<RefMapTest.Thing> actual = map.toSet();
        check(actual, expected);

        RefMapTest.Thing extra = new RefMapTest.Thing(size);
        assertTrue(actual.add(extra));
        expected.add(extra);
        check(actual, expected);
        assertTrue(map.containsKey(extra));
    }

}