package br.ufsc.lapesd.riefederator.util;

import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.*;

public class IndexedSetTest {
    private static final int VALUES_BASE = 1000;

    private static @Nonnull IndexedSet<Integer> set(int... values) {
        return new IndexedSet<>(Arrays.stream(values).boxed().collect(toList()));
    }

    private static @Nonnull List<Integer> randomValues(int size) {
        List<Integer> values = range(VALUES_BASE, VALUES_BASE+size).boxed().collect(toList());
        Collections.shuffle(values);
        return values;
    }

    @DataProvider
    public static Object[][] sizeData() {
        return Stream.of(
                asList(set(), 0),
                asList(set(1), 1),
                asList(set(2, 3), 2),
                asList(set(4, 5, 6), 3),
                asList(set(7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18), 12)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] sizesData() {
        return Stream.of(0, 1, 2, 3, 4, 7, 8, 16, 23, 31, 32, 64, 512)
                .map(i -> new Object[]{i}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "sizeData")
    public void testSize(IndexedSet<Integer> set, int expected) {
        assertEquals(set.size(), expected);
        assertEquals(set.isEmpty(), expected == 0);
    }

    @Test(dataProvider = "sizesData")
    public void testContainsIndexOfAndGet(int size) {
        List<Integer> values = randomValues(size);
        IndexedSet<Integer> set = IndexedSet.from(values);
        assertEquals(set.size(), values.size());
        for (Integer value : values) {
            assertTrue(set.contains(value));
            int idx = set.indexOf(value);
            assertTrue(idx >= 0);
            assertEquals(set.lastIndexOf(value), idx);
            assertEquals(set.get(idx), value);
        }
        assertTrue(set.containsAll(values));

        for (int i = -2; i < VALUES_BASE; i++) {
            assertEquals(set.indexOf(i), -1);
            assertEquals(set.lastIndexOf(i), -1);
            assertFalse(set.contains(i));
        }
        List<Integer> dummy = new ArrayList<>();
        expectThrows(IndexOutOfBoundsException.class, () -> dummy.add(set.get(VALUES_BASE-1)));
        for (int i = 0; i < 1024; i++) {
            int value = VALUES_BASE + size + i;
            assertEquals(set.indexOf(value), -1);
            assertEquals(set.lastIndexOf(value), -1);
            assertFalse(set.contains(value));
        }
        expectThrows(IndexOutOfBoundsException.class, () -> dummy.add(set.get(VALUES_BASE+size)));
        assertEquals(dummy, emptyList());
    }

    @Test(dataProvider = "sizesData")
    public void testEquals(int size) {
        List<Integer> values = randomValues(size);
        IndexedSet<Integer> set = IndexedSet.from(values);

        for (int i = 0; i < 16; i++) {
            ArrayList<Integer> permutation = new ArrayList<>(values);
            Collections.shuffle(permutation);
            IndexedSet<Integer> other = IndexedSet.from(permutation);
            assertTrue(set.containsAll(other));
            assertTrue(other.containsAll(set));
            assertEquals(set, other);
            assertEquals(set.hashCode(), other.hashCode());
        }
    }

    @Test
    public void testNotEquals() {
        IndexedSet<Integer> a = set(), b = set(1, 2), c = set(1), d = set(1, 2, 3), e = set(1, 3);

        assertNotEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertNotEquals(a, e);

        assertNotEquals(b, c);
        assertNotEquals(b, d);
        assertNotEquals(b, e);

        assertNotEquals(c, d);
        assertNotEquals(c, e);

        assertNotEquals(d, e);
    }

    @Test(dataProvider = "sizesData")
    public void testCopying(int size) {
        List<Integer> values = randomValues(size);
        if (size <= 1) return; //nothing to test

        ArrayList<Integer> initial1 = new ArrayList<>(values);
        IndexedSet<Integer> copy1 = IndexedSet.from(values);
        Collections.reverse(values);
        ArrayList<Integer> initial2 = new ArrayList<>(values);
        IndexedSet<Integer> copy2 = IndexedSet.fromDistinctCopy(values);
        Collections.reverse(values);

        for (int i = 0; i < initial1.size(); i++) {
            assertEquals(copy1.indexOf(initial1.get(i)), i);
            assertEquals(copy1.get(copy1.indexOf(initial1.get(i))), initial1.get(i));
        }
        for (int i = 0; i < initial2.size(); i++) {
            assertEquals(copy2.indexOf(initial2.get(i)), i);
            assertEquals(copy2.get(copy2.indexOf(initial2.get(i))), initial2.get(i));
        }
    }

    @Test
    public void testNoCopy() {
        List<Integer> values = randomValues(3);
        ArrayList<Integer> initial = new ArrayList<>(values);
        IndexedSet<Integer> set = IndexedSet.fromDistinct(values);
        Collections.reverse(values);

        assertEquals(set.get(0), values.get(0));
        assertEquals(set.get(1), values.get(1));
        assertEquals(set.get(2), values.get(2));

        assertEquals(set.indexOf(initial.get(0)), 0);
        assertEquals(set.indexOf(initial.get(1)), 1);
        assertEquals(set.indexOf(initial.get(2)), 2);

        assertEquals(set.get(0), initial.get(2));
        assertEquals(set.get(1), initial.get(1));
        assertEquals(set.get(2), initial.get(0));
    }

    @SuppressWarnings("SimplifyStreamApiCallChains")
    @Test(dataProvider = "sizesData")
    public void testSpecialSubsets(int size) {
        List<Integer> values = randomValues(size);
        IndexedSet<Integer> set = IndexedSet.fromDistinct(values);

        IndexedSubset<Integer> empty = set.emptySubset();
        assertEquals(empty.getBitSet().cardinality(), 0);
        assertTrue(empty.isEmpty());
        assertEquals(empty.size(), 0);
        assertFalse(empty.iterator().hasNext());
        //noinspection SimplifyStreamApiCallChains
        assertEquals(empty.stream().collect(toSet()), emptySet());
        assertEquals(set.stream().filter(empty::contains).collect(toList()), emptyList());

        IndexedSubset<Integer> all = set.fullSubset();
        assertEquals(all.getBitSet().cardinality(), size);
        assertEquals(all.isEmpty(), size == 0);
        assertEquals(all.size(), size);
        assertEquals(all.iterator().hasNext(), size > 0);

        List<Integer> iterated = new ArrayList<>();
        all.iterator().forEachRemaining(iterated::add);
        assertEquals(iterated, values);

        assertEquals(all.stream().collect(toSet()), new HashSet<>(values));
        assertEquals(all.stream().collect(toList()), values);
        assertEquals(set.stream().filter(all::contains).collect(toList()), values);
        assertTrue(all.containsAll(set));
        assertTrue(set.containsAll(all));

        IndexedSubset<Integer> empty2 = set.emptySubset();
        assertEquals(empty, empty2);
        assertEquals(empty.hashCode(), empty2.hashCode());

        IndexedSubset<Integer> all2 = set.fullSubset();
        assertEquals(all, all2);
        assertEquals(all.hashCode(), all2.hashCode());

        if (size > 0)
            assertNotEquals(empty, all);

        empty2.intersect(all);
        assertEquals(empty2, empty);

        all2.intersect(all);
        assertEquals(all2, all);

        all2.intersect(empty2);
        assertEquals(all2, empty);
    }

    @Test
    public void testCopySubset() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> empty = set.emptySubset();
        IndexedSubset<Integer> sub = empty.copy();
        sub.add(10);
        assertEquals(sub.size(), 1);
        assertEquals(empty.size(), 0);
        assertEquals(new ArrayList<>(sub), singletonList(10));
        assertEquals(new ArrayList<>(empty), emptyList());
    }

    @Test
    public void testAddExtraneousToSubset() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> sub = set.emptySubset();
        sub.add(11);
        assertEquals(new ArrayList<>(sub), singletonList(11));
        expectThrows(IllegalArgumentException.class, () -> sub.add(13));
    }

    @Test
    public void testSubsetRemoveExtraneous() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> sub = set.emptySubset();
        sub.addAll(asList(11, 12));

        assertFalse(sub.remove(13));
        assertEquals(new ArrayList<>(sub), asList(11, 12));
        assertFalse(sub.remove(10));
        assertEquals(new ArrayList<>(sub), asList(11, 12));
        assertTrue(sub.remove(11));
        assertEquals(new ArrayList<>(sub), singletonList(12));
    }

    @Test
    public void testSubsetContains() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> sub = set.emptySubset();
        sub.addAll(asList(10, 12));

        assertTrue(sub.contains(10));
        assertFalse(sub.contains(11));
        assertTrue(sub.contains(12));
        assertFalse(sub.contains(13));
    }

    @SuppressWarnings({"SimplifiedTestNGAssertion", "EqualsBetweenInconvertibleTypes",
                       "EqualsIncompatibleType"})
    @Test
    public void testSubsetEqualsToArray() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> sub = set.fullSubset();

        assertEquals(sub, asList(10, 11, 12));
        assertTrue(sub.equals(asList(10, 11, 12)));
//        assertTrue(asList(10, 11, 12).equals(sub));

        //bad order is OK (but won't work by List.equals() implementation
//        assertEquals(sub, asList(12, 11, 10));
        assertTrue(sub.equals(asList(12, 11, 10)));
//        assertTrue(asList(12, 11, 10).equals(sub));

        sub.remove(11);
//        assertEquals(sub, asList(12, 10));
        assertTrue(sub.equals(asList(12, 10)));
//        assertTrue(asList(12, 10).equals(sub));
    }

    @SuppressWarnings({"SimplifiedTestNGAssertion"})
    @Test
    public void testSubsetEqualsToSet() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> sub = set.fullSubset();

        assertEquals(sub, Sets.newHashSet(10, 11, 12));
        assertTrue(sub.equals(Sets.newHashSet(10, 11, 12)));
        assertTrue(Sets.newHashSet(10, 11, 12).equals(sub));

        //bad order is OK
        assertEquals(sub, Sets.newHashSet(12, 11, 10));
        assertTrue(sub.equals(Sets.newHashSet(12, 11, 10)));
        assertTrue(Sets.newHashSet(12, 11, 10).equals(sub));

        sub.remove(11);
        assertEquals(sub, Sets.newHashSet(12, 10));
        assertTrue(sub.equals(Sets.newHashSet(12, 10)));
        assertTrue(Sets.newHashSet(12, 10).equals(sub));
    }

    @Test
    public void testSetOperations() {
        IndexedSet<Integer> set = set(10, 11, 12, 13, 14, 15);
        IndexedSubset<Integer> full = set.fullSubset();
        IndexedSubset<Integer> emptySubset = set.emptySubset();

        IndexedSubset<Integer> s0 = emptySubset.copy();
        s0.add(10);
        assertNotEquals(s0, emptySubset);
        assertEquals(new ArrayList<>(s0), singletonList(10));
        s0.intersect(full);
        assertEquals(new ArrayList<>(s0), singletonList(10));
        s0.intersect(emptySubset);
        assertEquals(s0, emptySubset);

        IndexedSubset<Integer> s13 = emptySubset.copy();
        s13.add(11);
        s13.add(13);
        s0.add(10);
        assertEquals(s0.createIntersection(s13), emptySubset);

        IndexedSubset<Integer> s013 = s0.createUnion(s13);
        IndexedSubset<Integer> s013_ = emptySubset.copy();
        s013_.addAll(asList(13, 11, 10));
        assertEquals(new ArrayList<>(s013_), asList(10, 11, 13));
        assertEquals(s013, s013_);

        assertEquals(s013.createIntersection(full), s013);
        assertEquals(s013.createIntersection(full), s013_);
        assertEquals(s013.createIntersection(s13), s13);
        assertEquals(s013.createIntersection(s0), s0);
        assertEquals(s013.createIntersection(emptySubset), emptySubset);
        assertEquals(s013.createUnion(full), full);
        assertEquals(new ArrayList<>(s013), asList(10, 11, 13));

        IndexedSubset<Integer> s35 = emptySubset.copy();
        s35.addAll(asList(13, 15));
        assertEquals(new ArrayList<>(s35), asList(13, 15));
        assertEquals(new ArrayList<>(s35.createIntersection(s013)), singletonList(13));
        assertEquals(new ArrayList<>(s35.createUnion(s013)), asList(10, 11, 13, 15));
    }

}