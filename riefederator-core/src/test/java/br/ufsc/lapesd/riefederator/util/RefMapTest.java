package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static java.lang.Integer.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Comparator.naturalOrder;
import static org.testng.Assert.*;

public class RefMapTest {
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

    private interface Factory {
        @Nonnull Map<Thing, Integer> create();
        @Nonnull Map<Thing, Integer> create(int capacity);
        @Nonnull Map<Thing, Integer> create(@Nonnull Map<Thing, Integer> other);
    }

    private static List<Factory> factories = asList(
            new Factory() {
                @Override public @Nonnull Map<Thing, Integer> create() {
                    return new RefHashMap<>();
                }
                @Override public @Nonnull Map<Thing, Integer> create(int capacity) {
                    return new RefHashMap<>(capacity);
                }
                @Override
                public @Nonnull Map<Thing, Integer> create(@Nonnull Map<Thing, Integer> other) {
                    return new RefHashMap<>(other);
                }
                @Override public String toString() { return "RefHashMap"; }
            }
    );

    private void checkReadOperations(@Nonnull Map<Thing, Integer> actual,
                                     @Nonnull Map<Thing, Integer> expected) {
        assertEquals(actual.size(), expected.size());
        assertEquals(actual.isEmpty(), expected.isEmpty());

        checkGet(actual, expected);
        checkKeysAccess(actual, expected);
        checkValuesAccess(actual, expected);
        checkEquality(actual, expected);
        checkIteration(actual, expected);
    }

    private void checkIteration(@Nonnull Map<Thing, Integer> actual,
                                @Nonnull Map<Thing, Integer> expected) {
        List<Map.Entry<Thing, Integer>> entryList = new ArrayList<>();
        Iterator<Map.Entry<Thing, Integer>> entryIt = actual.entrySet().iterator();
        while (entryIt.hasNext())
            entryList.add(entryIt.next());
        assertEquals(entryList.size(), expected.entrySet().size());
        assertEquals(entryList.size(), expected.size());
        ArrayList<Map.Entry<Thing, Integer>> exEntryList = new ArrayList<>(expected.entrySet());
        assertTrue(entryList.containsAll(exEntryList));
        assertTrue(exEntryList.containsAll(entryList));

        List<Thing> keyList = new ArrayList<>();
        Iterator<Thing> keyIt = actual.keySet().iterator();
        while (keyIt.hasNext())
            keyList.add(keyIt.next());
        assertEquals(keyList.size(), expected.keySet().size());
        assertEquals(keyList.size(), expected.size());
        ArrayList<Thing> exKeyList = new ArrayList<>(expected.keySet());
        assertTrue(keyList.containsAll(exKeyList));
        assertTrue(exKeyList.containsAll(keyList));
    }

    private void checkGet(@Nonnull Map<Thing, Integer> actual,
                          @Nonnull Map<Thing, Integer> expected) {
        ArrayList<Map.Entry<Thing, Integer>> list = new ArrayList<>(expected.entrySet());
        for (Map.Entry<Thing, Integer> e : list)
            assertEquals(actual.get(e.getKey()), e.getValue());
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list)
            assertEquals(actual.get(e.getKey()), e.getValue());
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list)
            assertNull(actual.get(new Thing(e.getKey().id)));
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list)
            assertNull(actual.get(new Thing(e.getKey().id)));
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list) {
            assertEquals(actual.getOrDefault(e.getKey(), null), e.getValue());
            assertEquals(actual.getOrDefault(e.getKey(), MAX_VALUE), e.getValue());
            Thing copy = new Thing(e.getKey().id);
            assertEquals(actual.getOrDefault(copy, MAX_VALUE), Integer.valueOf(MAX_VALUE));
        }
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list) {
            assertEquals(actual.getOrDefault(e.getKey(), null), e.getValue());
            assertEquals(actual.getOrDefault(e.getKey(), MAX_VALUE), e.getValue());
            Thing copy = new Thing(e.getKey().id);
            assertEquals(actual.getOrDefault(copy, MAX_VALUE), Integer.valueOf(MAX_VALUE));
        }

        Thing lower = new Thing(expected.keySet().stream()
                .min(Comparator.comparing(Thing::getId)).map(Thing::getId).orElse(0) - 1);
        assertNull(actual.get(lower));
        Thing upper = new Thing(expected.keySet().stream()
                .max(Comparator.comparing(Thing::getId)).map(Thing::getId).orElse(0) + 1);
        assertNull(actual.get(upper));
    }

    @SuppressWarnings({"SimplifiableAssertion", "SimplifyStreamApiCallChains"})
    private void checkEquality(@Nonnull Map<Thing, Integer> actual,
                               @Nonnull Map<Thing, Integer> expected) {
        // hash code of actual cannot be compared with that of expected because actual will
        // not use Key::hashCode. These two invariants should hold, however.
        assertEquals(expected.entrySet().hashCode(), expected.hashCode());
        assertEquals(actual.entrySet().hashCode(), actual.hashCode());

        // sizes are comparable
        assertEquals(actual.entrySet().size(), expected.entrySet().size());
        assertEquals(actual.size(), expected.size());
        assertEquals(actual.keySet().size(), expected.keySet().size());
        assertEquals(actual.values().size(), expected.values().size());

        // Despite not using the same hash function, the entries are equal()
        ArrayList<Map.Entry<Thing, Integer>> expectedEntryList = new ArrayList<>(expected.entrySet());
        assertTrue(actual.entrySet().stream().allMatch(expectedEntryList::contains));

        // contains() implementation does not rely on hashing
        for (Map.Entry<Thing, Integer> e : expected.entrySet()) {
            if (!actual.entrySet().contains(e))
                assertTrue(actual.entrySet().contains(e));
        }
        assertTrue(expected.entrySet().stream().allMatch(actual.entrySet()::contains));
        assertTrue(actual.entrySet().containsAll(expected.entrySet()));

        // values use the same hashing function
        assertTrue(new HashSet<>(actual.values()).equals(new HashSet<>(expected.values())));
        assertTrue(new HashSet<>(expected.values()).equals(new HashSet<>(actual.values())));
    }

    private void checkKeysAccess(@Nonnull Map<Thing, Integer> actual,
                                 @Nonnull Map<Thing, Integer> expected) {
        for (Thing key : expected.keySet())
            assertTrue(actual.containsKey(key));
        assertEquals(actual.keySet().size(), expected.keySet().size());
        assertEquals(new HashSet<>(actual.keySet()), new HashSet<>(expected.keySet()));

        for (Thing key : expected.keySet())
            assertFalse(actual.containsKey(new Thing(key.id)));

        Thing upper = new Thing(expected.keySet().stream()
                .max(Comparator.comparing(Thing::getId))
                .map(Thing::getId).orElse(0) + 1);
        Thing lower = new Thing(expected.keySet().stream()
                .min(Comparator.comparing(Thing::getId))
                .map(Thing::getId).orElse(0) + 1);
        assertFalse(actual.containsKey(upper));
        assertFalse(actual.containsKey(lower));
    }

    private void checkValuesAccess(@Nonnull Map<Thing, Integer> actual,
                                   @Nonnull Map<Thing, Integer> expected) {
        HashSet<Integer> values = new HashSet<>(expected.values());
        for (Integer value : values)
            assertTrue(actual.containsValue(value));
        int min = values.stream().min(naturalOrder()).orElse(0) - 10;
        int max = values.stream().max(naturalOrder()).orElse(0) + 10;
        for (int i = min; i < max; i++) {
            assertEquals(actual.containsValue(i), values.contains(i));
        }
        assertEquals(actual.values().size(), expected.values().size());
        assertEquals(new HashSet<>(actual.values()), new HashSet<>(expected.values()));
    }

    @DataProvider
    public static @Nonnull Object[][] sizesData() {
        return Stream.of(0, 1, 2, 3, 10, 32)
                .flatMap(i -> factories.stream().map(f -> new Object[]{f, i}))
                .toArray(Object[][]::new);
    }

    @DataProvider
    public static @Nonnull Object[][] factoriesData() {
        return factories.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testEmptyReserved(Factory fac, int size) {
        Map<Thing, Integer> actual = fac.create(size);
        Map<Thing, Integer> expected = new HashMap<>();
        checkReadOperations(actual, expected);
    }

    private @Nonnull Map<Thing, Integer> createMap(int size) {
        Map<Thing, Integer> map = new HashMap<>();
        for (int i = 0; i < size; i++)
            map.put(new Thing(i), i);
        return map;
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testCopy(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create(expected);
        checkReadOperations(actual, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testFill(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create();
        for (Map.Entry<Thing, Integer> e : expected.entrySet())
            assertNull(actual.put(e.getKey(), e.getValue()));
        assertEquals(actual.entrySet().iterator().hasNext(), size > 0);
        checkReadOperations(actual, expected);
    }

    @Test(dataProvider = "factoriesData")
    public void testFillLarge(Factory fac) {
        Map<Thing, Integer> expected = createMap(4096);
        Map<Thing, Integer> actual = fac.create();
        for (Map.Entry<Thing, Integer> e : expected.entrySet())
            assertNull(actual.put(e.getKey(), e.getValue()));
        checkReadOperations(actual, expected);
    }



    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testFillReserved(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create(size);
        for (Map.Entry<Thing, Integer> e : expected.entrySet())
            assertNull(actual.put(e.getKey(), e.getValue()));
        assertEquals(actual.entrySet().iterator().hasNext(), size > 0);
        checkReadOperations(actual, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testCopyClearCopy(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create();
        int expectedSize =  0;
        for (Map.Entry<Thing, Integer> e : expected.entrySet()) {
            assertNull(actual.put(e.getKey(), e.getValue()));
            assertEquals(actual.size(), ++expectedSize);
        }
        checkReadOperations(actual, expected);

        // re-put in same order
        for (Map.Entry<Thing, Integer> e : expected.entrySet()) {
            assertEquals(actual.put(e.getKey(), e.getValue()), e.getValue());
            assertEquals(actual.size(), expectedSize);
        }
        checkReadOperations(actual, expected);

        // repeat in reverse order
        ArrayList<Map.Entry<Thing, Integer>> list = new ArrayList<>(expected.entrySet());
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list) {
            assertEquals(actual.put(e.getKey(), e.getValue()), e.getValue());
            assertEquals(actual.size(), expectedSize);
        }
        checkReadOperations(actual, expected);

        // add in reverse order after clear
        actual.clear();
        expectedSize = 0;
        for (Map.Entry<Thing, Integer> e : list) {
            assertNull(actual.put(e.getKey(), e.getValue()));
            assertEquals(actual.size(), ++expectedSize);
        }
        checkReadOperations(actual, expected);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveByKeyLeftToRight(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create();
        actual.putAll(expected);
        checkReadOperations(actual, expected);

        for (Map.Entry<Thing, Integer> e : expected.entrySet())
            assertEquals(actual.remove(e.getKey()), e.getValue());
        checkReadOperations(actual, emptyMap());

        for (Map.Entry<Thing, Integer> e : expected.entrySet())
            assertNull(actual.remove(e.getKey()));
        checkReadOperations(actual, emptyMap());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveByKeyRightToLeft(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create();
        actual.putAll(expected);
        checkReadOperations(actual, expected);

        ArrayList<Map.Entry<Thing, Integer>> list = new ArrayList<>(expected.entrySet());
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> e : list) {
            assertEquals(actual.remove(e.getKey()), e.getValue());
            assertNull(actual.remove(e.getKey()));
        }
        checkReadOperations(actual, emptyMap());

        for (Map.Entry<Thing, Integer> e : list)
            assertNull(actual.remove(e.getKey()));
        checkReadOperations(actual, emptyMap());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testClearEntries(Factory fac, int size) {
        Map<Thing, Integer> actual = fac.create(createMap(size));
        actual.entrySet().clear();
        checkReadOperations(actual, emptyMap());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testClearKeys(Factory fac, int size) {
        Map<Thing, Integer> actual = fac.create(createMap(size));
        actual.keySet().clear();
        checkReadOperations(actual, emptyMap());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllKeys(Factory fac, int size) {
        Map<Thing, Integer> in = createMap(size);
        Map<Thing, Integer> actual = fac.create(in);
        actual.keySet().removeAll(in.keySet());
        checkReadOperations(actual, emptyMap());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllKeysReverse(Factory fac, int size) {
        Map<Thing, Integer> in = createMap(size);
        Map<Thing, Integer> actual = fac.create(in);
        ArrayList<Thing> list = new ArrayList<>(in.keySet());
        Collections.reverse(list);
        actual.keySet().removeAll(list);
        checkReadOperations(actual, emptyMap());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllEntries(Factory fac, int size) {
        Map<Thing, Integer> in = createMap(size);
        Map<Thing, Integer> actual = fac.create(in);
        actual.entrySet().removeAll(in.entrySet());
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllEntriesReverse(Factory fac, int size) {
        Map<Thing, Integer> in = createMap(size);
        Map<Thing, Integer> actual = fac.create(in);
        ArrayList<Map.Entry<Thing, Integer>> list = new ArrayList<>(in.entrySet());
        Collections.reverse(list);
        actual.entrySet().removeAll(list);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllIterator(Factory fac, int size) {
        Map<Thing, Integer> initial = createMap(size);
        Map<Thing, Integer> removed = new HashMap<>();
        Map<Thing, Integer> expected = new HashMap<>(initial);
        Map<Thing, Integer> actual = fac.create(expected);

        Iterator<Map.Entry<Thing, Integer>> it = actual.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Thing, Integer> e = it.next();
            assertEquals(expected.remove(e.getKey()), e.getValue());
            it.remove();
            assertNull(removed.put(e.getKey(), e.getValue()));
            checkReadOperations(actual, expected);
        }
        assertEquals(removed, initial);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveAllKeyIterator(Factory fac, int size) {
        Map<Thing, Integer> initial = createMap(size);
        Map<Thing, Integer> removed = new HashMap<>();
        Map<Thing, Integer> expected = new HashMap<>(initial);
        Map<Thing, Integer> actual = fac.create(expected);

        Iterator<Thing> it = actual.keySet().iterator();
        while (it.hasNext()) {
            Thing key = it.next();
            Integer value = actual.get(key);
            assertEquals(value, initial.get(key));
            assertEquals(expected.remove(key), initial.get(key));
            it.remove();
            assertNull(removed.put(key, value));
            checkReadOperations(actual, expected);
        }
        assertEquals(removed, initial);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testRemoveEven(Factory fac, int size) {
        Map<Thing, Integer> initial = createMap(size);
        Map<Thing, Integer> removed = new HashMap<>();
        Map<Thing, Integer> expected = new HashMap<>(initial);
        Map<Thing, Integer> actual = fac.create(expected);

        while (!actual.isEmpty()) {
            Iterator<Map.Entry<Thing, Integer>> it = actual.entrySet().iterator();
            for (int i = 0; it.hasNext(); i++) {
                Map.Entry<Thing, Integer> e = it.next();
                if (i % 2 == 0) {
                    assertEquals(expected.remove(e.getKey()), e.getValue());
                    it.remove();
                    assertNull(removed.put(e.getKey(), e.getValue()));
                }
                checkReadOperations(actual, expected);
            }
        }
        assertEquals(removed, initial);
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testDoubleValuesLeftToRight(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create(expected);

        for (Map.Entry<Thing, Integer> entry : actual.entrySet()) {
            entry.setValue(entry.getValue()*2);
            Thing key = entry.getKey();
            expected.put(key, expected.get(key)*2);
            checkReadOperations(actual, expected);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testDoubleValuesRightToLeft(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create(expected);

        ArrayList<Map.Entry<Thing, Integer>> list = new ArrayList<>(actual.entrySet());
        Collections.reverse(list);
        for (Map.Entry<Thing, Integer> entry : list) {
            entry.setValue(entry.getValue()*2);
            Thing key = entry.getKey();
            expected.put(key, expected.get(key)*2);
            checkReadOperations(actual, expected);
        }
    }

    @Test(dataProvider = "sizesData", groups = {"fast"})
    public void testDoubleValuesIterator(Factory fac, int size) {
        Map<Thing, Integer> expected = createMap(size);
        Map<Thing, Integer> actual = fac.create(expected);

        Iterator<Map.Entry<Thing, Integer>> it = actual.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Thing, Integer> e = it.next();
            Thing key = e.getKey();
            e.setValue(e.getValue()*2);
            expected.put(key, expected.get(key)*2);
            checkReadOperations(actual, expected);
        }
    }




}