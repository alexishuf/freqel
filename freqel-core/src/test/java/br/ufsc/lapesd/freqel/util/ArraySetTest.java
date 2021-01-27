package br.ufsc.lapesd.freqel.util;

import br.ufsc.lapesd.freqel.NamedSupplier;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class ArraySetTest {
    private static final List<NamedSupplier<Collection<Integer>>> suppliers = Arrays.asList(
            new NamedSupplier<>("ArrayList", ArrayList::new),
            new NamedSupplier<>("HashSet", HashSet::new),
            new NamedSupplier<>("TreeSet", TreeSet::new)
    );

    @DataProvider
    public static Object[][] sequentialData() {
        return Stream.of(0, 1, 2, 3, 4, 8, 10, 16, 37, 128, 1024, 3000)
                .flatMap(i -> suppliers.stream().map(s -> new Object[]{i, s}))
                .toArray(Object[][]::new);
    }

    @SuppressWarnings({"SimplifiedTestNGAssertion", "SimplifyStreamApiCallChains"})
    @Test(dataProvider = "sequentialData")
    public void testComparison(int size, Supplier<Collection<Integer>> supplier) {
        Collection<Integer> collection = supplier.get();
        for (int i = 0; i < size; i++)
            collection.add(i);
        ArrayList<Integer> shuffled = new ArrayList<>(collection);
        Collections.shuffle(shuffled);
        assertEquals(collection.size(), shuffled.size());

        ArraySet<Integer> fromColl = ArraySet.fromDistinct(collection);
        ArraySet<Integer> fromShuffled = ArraySet.fromDistinct(shuffled);
        ArraySet<Integer> fromIt = ArraySet.fromDistinct(collection.iterator());

        assertEquals(fromColl.size(), collection.size());
        assertEquals(fromIt.size(), collection.size());
        assertEquals(fromShuffled.size(), collection.size());

        assertEquals(fromColl.isEmpty(), collection.isEmpty());
        assertEquals(fromIt.isEmpty(), collection.isEmpty());
        assertEquals(fromShuffled.isEmpty(), collection.isEmpty());

        assertTrue(fromColl.equals(fromIt));
        assertTrue(fromIt.equals(fromColl));
        assertTrue(fromColl.equals(collection));
        assertTrue(fromIt.equals(collection));
        assertTrue(fromShuffled.equals(fromColl));
        assertTrue(fromIt.equals(fromShuffled));

        assertTrue(new HashSet<>(collection).equals(fromColl));
        assertTrue(new HashSet<>(collection).equals(fromIt));
        assertTrue(new HashSet<>(collection).equals(fromShuffled));
        assertTrue(new HashSet<>(shuffled).equals(fromColl));
        assertTrue(new HashSet<>(shuffled).equals(fromIt));
        assertTrue(new HashSet<>(shuffled).equals(fromShuffled));

        assertEquals(fromColl.hashCode(), fromIt.hashCode());
        assertEquals(fromColl.hashCode(), fromShuffled.hashCode());

        assertTrue(collection.stream().allMatch(fromColl::contains));
        assertTrue(collection.stream().allMatch(fromIt::contains));
        assertTrue(collection.stream().allMatch(fromShuffled::contains));

        assertTrue(fromColl.containsAll(fromIt));
        assertTrue(fromIt.containsAll(fromColl));
        assertTrue(fromColl.containsAll(fromShuffled));
        assertTrue(fromShuffled.containsAll(fromColl));
        assertTrue(fromIt.containsAll(fromShuffled));
        assertTrue(fromShuffled.containsAll(fromIt));

        assertTrue(fromColl.containsAll(collection));
        assertTrue(collection.containsAll(fromColl));
        assertTrue(fromIt.containsAll(collection));
        assertTrue(collection.containsAll(fromIt));
        assertTrue(fromShuffled.containsAll(collection));
        assertTrue(collection.containsAll(fromShuffled));

        assertTrue(fromColl.containsAll(shuffled));
        assertTrue(shuffled.containsAll(fromColl));
        assertTrue(fromIt.containsAll(shuffled));
        assertTrue(shuffled.containsAll(fromIt));
        assertTrue(fromShuffled.containsAll(shuffled));
        assertTrue(shuffled.containsAll(fromShuffled));
    }

    @Test(dataProvider = "sequentialData")
    public void testIterate(int size, Supplier<Collection<Integer>> supplier) {
        Collection<Integer> collection = supplier.get();
        for (int i = 0; i < size; i++)
            collection.add(i);
        ArrayList<Integer> shuffled = new ArrayList<>(collection);
        Collections.shuffle(shuffled);
        assertEquals(collection.size(), shuffled.size());

        ArraySet<Integer> fromColl = ArraySet.fromDistinct(collection);
        ArraySet<Integer> fromShuffled = ArraySet.fromDistinct(shuffled);
        ArraySet<Integer> fromIt = ArraySet.fromDistinct(collection.iterator());

        List<Integer> sortedList = new ArrayList<>(collection), actual = new ArrayList<>();
        sortedList.sort(Comparator.naturalOrder());

        //noinspection CollectionAddAllCanBeReplacedWithConstructor
        actual.addAll(fromColl);
        assertEquals(actual, sortedList);

        actual.clear();
        actual.addAll(fromIt);
        assertEquals(actual, sortedList);

        actual.clear();
        actual.addAll(fromShuffled);
        assertEquals(actual, sortedList);
    }

}