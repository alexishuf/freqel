package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class RefSetTest {

    public static class Thing {
        private int id;

        public Thing(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Thing)) return false;
            Thing thing = (Thing) o;
            return id == thing.id;
        }

        @Override public int hashCode() {
            return Objects.hash(id);
        }

        @Override public @Nonnull String toString() {
            return String.format("Thing(%d)", id);
        }
    }


    interface Factory<T> {
        RefSet<T> create();
        RefSet<T> create(int capacity);
    }

    private static List<Factory<Thing>> factories = Collections.singletonList(
            new Factory<Thing>() {
                @Override public RefSet<Thing> create() {
                    return new IdentityHashSet<>();
                }
                @Override public RefSet<Thing> create(int capacity) {
                    return new IdentityHashSet<>(capacity);
                }

                @Override public String toString() {
                    return "IdentityHashSet";
                }
            }
    );

    private void check(@Nonnull Set<Thing> actual,
                       @Nonnull Set<Thing> expected) {
        checkEquality(actual, expected);
        checkIteration(actual, expected);
        checkAddIdempotency(actual, expected);
        checkNegativeContains(actual, expected);
    }

    private void checkNegativeContains(@Nonnull Set<Thing> actual, @Nonnull Set<Thing> expected) {
        for (Thing thing : expected) {
            Thing copy = new Thing(thing.getId());
            assertFalse(actual.contains(copy));
        }
    }

    private void checkAddIdempotency(@Nonnull Set<Thing> actual, @Nonnull Set<Thing> expected) {
        for (Thing thing : expected) {
            assertTrue(actual.contains(thing));
            assertFalse(actual.add(thing));
        }
        checkEquality(actual, expected);
    }

    private void checkIteration(@Nonnull Set<Thing> actual, @Nonnull Set<Thing> expected) {
        List<Thing> observed = new ArrayList<>();
        for (Iterator<Thing> it = actual.iterator(); it.hasNext(); )
            observed.add(it.next());
        assertEquals(observed.size(), expected.size());
        assertEquals(new HashSet<>(observed), expected);
    }

    private void checkEquality(@Nonnull Set<Thing> actual, @Nonnull Set<Thing> expected) {
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
                .flatMap(i -> factories.stream().map(f -> new Object[] {f, i}))
                .toArray(Object[][]::new);
    }

    private void fill(@Nonnull Set<Thing> actual,
                      @Nonnull Set<Thing> expected,
                      int from, int to, int step) {
        if (step < 0) {
            assert to <= from;
            for (int i = from; i > to; i += step) {
                Thing thing = new Thing(i);
                assertTrue(actual.add(thing));
                assertFalse(actual.add(thing));
                assertTrue(expected.add(thing));
            }
        } else {
            assert to >= from;
            for (int i = from; i < to; i += step) {
                Thing thing = new Thing(i);
                assertTrue(actual.add(thing));
                assertFalse(actual.add(thing));
                assertTrue(expected.add(thing));
            }
        }
        check(actual, expected);
    }

    @Test(dataProvider = "sizesData")
    public void testFill(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        fill(actual, new HashSet<>(), 0, size, 1);
    }

    @Test(dataProvider = "sizesData")
    public void testFillReserved(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create(size);
        fill(actual, new HashSet<>(), 0, size, 1);
    }

    @Test(dataProvider = "sizesData")
    public void testFillUnderReserved(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create(size / 2);
        fill(actual, new HashSet<>(), 0, size, 1);
    }

    @Test(dataProvider = "sizesData")
    public void testFillEven(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        fill(actual, new HashSet<>(), 0, size, 2);
    }

    @Test(dataProvider = "sizesData")
    public void testFillBackwards(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        fill(actual, new HashSet<>(), size, -1, -1);
    }

    @Test(dataProvider = "sizesData")
    public void testClear(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        fill(actual, new HashSet<>(), size, -1, -1);
        actual.clear();
        check(actual, emptySet());
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAll(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        HashSet<Thing> expected = new HashSet<>();
        fill(actual, expected, size, -1, -1);
        assertTrue(actual.removeAll(expected));
        check(actual, emptySet());
        assertFalse(actual.removeAll(expected));
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAllManual(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        HashSet<Thing> expected = new HashSet<>();
        fill(actual, expected, size, -1, -1);
        for (Thing thing : expected)
            assertTrue(actual.remove(thing));
        check(actual, emptySet());

        for (Thing thing : expected)
            assertFalse(actual.remove(thing));
        assertFalse(actual.removeAll(expected));
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveByIterator(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        HashSet<Thing> expected = new HashSet<>();
        fill(actual, expected, size, -1, -1);
        Iterator<Thing> it = actual.iterator();
        while (it.hasNext()) {
            Thing thing = it.next();
            assertTrue(expected.contains(thing));
            it.remove();
            expected.remove(thing);
            check(actual, expected);
        }
        assertTrue(expected.isEmpty());
        check(actual, emptySet());
    }

    @Test(dataProvider = "sizesData")
    public void testStableHash(Factory<Thing> factory, int size) {
        RefSet<Thing> actual = factory.create();
        HashSet<Thing> expected = new HashSet<>();
        fill(actual, expected, 0, size, 1);
        RefSet<Thing> actual2 = factory.create(actual.size());
        actual2.addAll(actual);
        HashSet<Thing> expected2 = new HashSet<>(expected);

        assertEquals(expected2.hashCode(), expected.hashCode());
        assertEquals(actual2.hashCode(), actual.hashCode());
    }
}