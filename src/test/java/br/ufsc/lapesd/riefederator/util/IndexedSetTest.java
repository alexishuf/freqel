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
        return IndexedSet.fromDistinct(Arrays.stream(values).boxed().collect(toList()));
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

    @Test(dataProvider = "sizesData")
    public void testFromMap(int size) {
        Map<Integer, Integer> map = new HashMap<>(size);
        List<Integer> list = new ArrayList<>(size);
        List<Integer> list2 = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            map.put(VALUES_BASE+i, i);
            list.add(VALUES_BASE+i);
            list2.add(VALUES_BASE+i);
        }
        IndexedSet<Integer> set = IndexedSet.fromMap(map, list);

        assertEquals(set.size(), size);
        assertEquals(new ArrayList<>(set), list2);
        assertEquals(set, new HashSet<>(list2));

        for (int i = 0; i < size; i++)
            assertEquals(set.get(i), list2.get(i));
        for (Iterator<Integer> it = set.iterator(), it2 = list2.iterator(); it.hasNext(); ) {
            assertTrue(it2.hasNext());
            assertEquals(it.next(), it2.next());
        }
    }

    @Test
    public void testMapTwoToOne() {
        Map<Integer, Integer> map = new HashMap<>();
        List<Integer> list = asList(10, 11, 12, 13, 14, 15);
        for (int i = 0; i < 6; i++) {
            map.put(10+i, i);
            map.put(20+i, i);
        }
        IndexedSet<Integer> set = IndexedSet.fromMap(map, list);

        assertEquals(set.size(), list.size());
        assertEquals(new ArrayList<>(set), list);
        assertEquals(set, new HashSet<>(list));

        for (int i = 0; i < set.size(); i++) {
            assertEquals(set.get(i), list.get(i));
            assertTrue(set.contains(list.get(i)));
            assertEquals(set.indexOf(list.get(i)), i);
        }
        for (int i = 0; i < set.size(); i++) {
            assertTrue(set.contains(20+i));
            assertEquals(set.indexOf(20+i), i);
            assertFalse(set.contains(30+i));
            assertEquals(set.indexOf(20+i), i);
        }

        assertFalse(set.contains(16));
        assertFalse(set.contains(19));
        assertFalse(set.containsAny(asList(16, 17, 18, 19)));
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

    @Test(dataProvider = "sizesData")
    public void testContainsAny(int size) {
        List<Integer> values = randomValues(size);
        IndexedSet<Integer> set = IndexedSet.fromDistinct(values);

        int step = size > 128 ? 4 : 1;
        for (int i = 0; i < size; i += step) {
            for (int j = 0; j < size; j += step) {
                assertTrue(set.containsAny(asList(values.get(i), values.get(j))));
                assertTrue(set.containsAny(asList(0, values.get(i), values.get(j))));
                assertTrue(set.containsAny(asList(values.get(i), values.get(j), 9)));
                assertTrue(set.containsAny(asList(0, values.get(i), values.get(j), 9)));
                assertTrue(set.containsAny(asList(0, 1, values.get(i), values.get(j))));
                assertTrue(set.containsAny(asList(0, 1, values.get(i), values.get(j))));

                assertTrue(set.containsAny(Sets.newHashSet(values.get(i), values.get(j))));
                assertTrue(set.containsAny(Sets.newHashSet(0, values.get(i), values.get(j))));
                assertTrue(set.containsAny(Sets.newHashSet(values.get(i), values.get(j), 9)));
                assertTrue(set.containsAny(Sets.newHashSet(0, values.get(i), values.get(j), 9)));
                assertTrue(set.containsAny(Sets.newHashSet(0, 1, values.get(i), values.get(j))));
                assertTrue(set.containsAny(Sets.newHashSet(0, 1, values.get(i), values.get(j))));
            }
        }

        Set<Integer> otherSet = new HashSet<>();
        List<Integer> otherList = new ArrayList<>(VALUES_BASE+2);
        for (int k = 0; k < VALUES_BASE; k++) {
            otherSet.add(k);
            otherList.add(k);
        }
        assertFalse(set.containsAny(otherSet));
        assertFalse(set.containsAny(otherList));

        if (size > 0) {
            otherSet.add(values.get(0));
            otherList.add(values.get(0));
            assertTrue(set.containsAny(otherSet));
            assertTrue(set.containsAny(otherList));
        }
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
    public void testIterateSubset() {
        IndexedSet<Integer> set = set(10, 11, 12, 13, 14);
        IndexedSubset<Integer> subset = set.subset(asList(13, 11));
        assertEquals(subset.size(), 2);

        Iterator<Integer> it = subset.iterator();
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(11));
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(13));
        assertFalse(it.hasNext());

        Iterator<Integer> it2 = subset.iterator();
        assertEquals(it2.next(), Integer.valueOf(11));
        assertEquals(it2.next(), Integer.valueOf(13));
        assertFalse(it2.hasNext());

        assertEquals(new ArrayList<>(subset), asList(11, 13));
        assertEquals(subset, Sets.newHashSet(11, 13));
    }


    @Test
    public void testIteratePairSubset() {
        IndexedSet<Integer> set = set(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        for (int i = 0; i < set.size(); i++) {
            for (int j = 0; j < set.size(); j++) {
                IndexedSubset<Integer> subset = set.subset(asList(10 + i, 10 + j));
                assertEquals(subset.size(), i==j ? 1 : 2);

                assertEquals(subset, Sets.newHashSet(10+i, 10+j));

                assertTrue(subset.contains(10+i));
                assertTrue(subset.contains(10+j));
                for (int k = 0; k < subset.size(); k++)
                    assertEquals(subset.contains(10+k), k == i || k == j);

                Integer lower = 10 + Math.min(i, j), upper = 10 + Math.max(i, j);
                Iterator<Integer> it = subset.iterator();
                assertTrue(it.hasNext());
                assertEquals(it.next(), lower);
                assertEquals(it.hasNext(), i != j);
                if (i != j) {
                    assertEquals(it.next(), upper);
                    assertFalse(it.hasNext());
                }

                Iterator<Integer> it2 = subset.iterator();
                assertEquals(it2.next(), lower);
                if (i != j)
                    assertEquals(it2.next(), upper);
                assertFalse(it2.hasNext());
            }
        }
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
    public void testFullSubsetContainsAll() {
        IndexedSet<Integer> set = set(10, 11, 12);
        IndexedSubset<Integer> full = set.fullSubset();
        assertTrue(full.containsAll(set));
        assertTrue(set.containsAll(set.fullSubset()));

        assertTrue(full.containsAll(emptyList()));
        assertTrue(full.containsAll(emptySet()));
        assertTrue(full.containsAll(set.emptySubset()));

        assertTrue(full.containsAll(asList(10, 11)));
        assertTrue(full.containsAll(asList(10, 11, 12)));
        assertFalse(full.containsAll(asList(10, 11, 12, 13)));

        assertTrue(full.containsAll(Sets.newHashSet(10, 11)));
        assertTrue(full.containsAll(Sets.newHashSet(10, 11, 12)));
        assertFalse(full.containsAll(Sets.newHashSet(10, 11, 12, 13)));

        IndexedSet<Integer> set2 = set(11, 12, 13);
        assertTrue(full.containsAll(set2.subset(asList(11, 12))));
        assertTrue(full.containsAll(set2.subset(12)));
        assertTrue(full.containsAll(set2.emptySubset()));
        assertFalse(full.containsAll(set2.subset(13)));
        assertFalse(full.containsAll(set2.fullSubset()));
        assertFalse(full.containsAll(set2));
    }

    @Test
    @SuppressWarnings("EqualsIncompatibleType")
    public void testSubsetContainsAll() {
        IndexedSet<Integer> set = set(10, 11, 12, 13), set2 = set(20, 21, 22, 11, 12, 14);
        IndexedSubset<Integer> sub = set.subset(asList(11, 12));
        //noinspection EqualsBetweenInconvertibleTypes,SimplifiedTestNGAssertion
        assertTrue(sub.equals(asList(12, 11)));
        assertEquals(sub, Sets.newHashSet(11, 12));

        assertTrue(sub.containsAll(asList(11, 12)));
        assertTrue(sub.containsAll(Sets.newHashSet(11, 12)));

        assertFalse(sub.containsAll(asList(10, 13)));
        assertFalse(sub.containsAll(asList(10, 12)));
        assertFalse(sub.containsAll(Sets.newHashSet(11, 13)));

        assertFalse(sub.containsAll(set2.fullSubset()));
        assertTrue(sub.containsAll(set2.emptySubset()));
        assertTrue(sub.containsAll(set2.subset(asList(12, 11))));
        assertTrue(sub.containsAll(set2.subset(12)));
        assertFalse(sub.containsAll(set2.subset(asList(12, 14))));
    }

    @Test
    public void testSubsetEquality() {
        IndexedSet<Integer> a = set(10, 11, 12), b = set(11, 12, 13);
        assertEquals(a.subset(10), a.subset(singletonList(10)));
        assertEquals(a.subset(10).hashCode(), a.subset(singletonList(10)).hashCode());

        assertNotEquals(a.subset(asList(10, 11)), a.subset(10));
        assertNotEquals(a.subset(asList(10, 11)), a.subset(11));

        assertNotEquals(a.subset(asList(10, 11)), b.subset(asList(11, 12)));
        assertEquals(a.subset(11), b.subset(11));
        assertEquals(a.subset(11).hashCode(), b.subset(11).hashCode());
        assertEquals(a.subset(asList(12, 11)), b.subset(asList(11, 12)));
    }

    @Test
    public void testSubsetHashCode() {
        IndexedSet<Integer> a = set(11, 12, 13, 14, 15), b = set(15, 14, 13, 12, 11);
        IndexedSet<Integer> c = set(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < a.size(); j++) {
                for (int k = 0; k < a.size(); k++) {
                    IndexedSubset<Integer> aSubset = a.subset(asList(i, j, k));
                    IndexedSubset<Integer> aSubset2 = a.subset(asList(j, i, k));
                    IndexedSubset<Integer> bSubset = b.subset(asList(i, j, k));
                    IndexedSubset<Integer> cSubset = c.subset(asList(i, j, k));

                    assertEquals(aSubset, aSubset2);
                    assertEquals(aSubset.hashCode(), aSubset2.hashCode());
                    assertEquals(aSubset, bSubset);
                    assertEquals(aSubset.hashCode(), bSubset.hashCode());
                    assertEquals(aSubset, cSubset);
                    assertEquals(aSubset.hashCode(), cSubset.hashCode());
                }
            }
        }
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

    @Test
    public void testSetDifference() {
        IndexedSet<Integer> set = set(10, 11, 12, 13, 14, 15);

        assertEquals(set.emptySubset().createDifference(set.fullSubset()), set.emptySubset());
        assertEquals(set.emptySubset().createDifference(set.subset(10)), set.emptySubset());
        assertEquals(set.fullSubset().createDifference(set.subset(11)),
                     set.subset(asList(10, 12, 13, 14, 15)));
        assertEquals(set.fullSubset().createDifference(set.subset(asList(11, 12, 13))),
                     set.subset(asList(10, 14, 15)));
        assertEquals(set.fullSubset().createDifference(set.subset(asList(11, 13, 15))),
                     set.subset(asList(10, 12, 14)));

        IndexedSubset<Integer> subset = set.fullSubset();
        assertEquals(subset.difference(set.subset(asList(10, 12, 14))), 3);
        assertEquals(subset, set.subset(asList(11, 13, 15)));

        subset = set.fullSubset();
        assertEquals(subset.difference(set.subset(asList(11, 13, 15))), 3);
        assertEquals(subset, set.subset(asList(10, 12, 14)));

        subset = set.subset(asList(10, 11, 12));
        assertEquals(subset.difference(set.subset(asList(11, 14, 15))), 1);
        assertEquals(subset, set.subset(asList(10, 12)));

        subset = set.subset(asList(11, 13, 15));
        assertEquals(subset.difference(asList(10, 11, 13)), 2);
        assertEquals(subset, set.subset(15));

        subset = set.subset(asList(10, 12, 14));
        assertEquals(subset.difference(set.fullSubset()), 3);
        assertEquals(subset, set.emptySubset());

        subset = set.subset(asList(11, 13, 15));
        assertEquals(subset.difference(set.subset(asList(15, 13, 11))), 3);
        assertEquals(subset, set.emptySubset());
    }


    @Test
    public void testUniversalEmpty() {
        IndexedSubset<Integer> e1 = IndexedSubset.empty();
        ImmutableIndexedSubset<Integer> e2 = ImmutableIndexedSubset.empty();
        IndexedSet<Integer> set = set(10, 11);
        IndexedSubset<Integer> e3 = set.emptySubset();
        ImmutableIndexedSubset<Integer> e4 = set.immutableEmptySubset();

        assertEquals(e1, e2);
        assertEquals(e1, e3);
        assertEquals(e1, e4);
        assertEquals(e2, e3);
        assertEquals(e2, e4);
        assertEquals(e3, e4);
    }

    @Test
    public void testImmutableSubset() {
        IndexedSet<Integer> set = set(10, 11, 12);
        ImmutableIndexedSubset<Integer> e = set.immutableEmptySubset();
        ImmutableIndexedSubset<Integer> s1 = set.immutableSubset(11);
        ImmutableIndexedSubset<Integer> s02 = set.immutableSubset(asList(10, 12));

        assertEquals(new ArrayList<>(e), emptyList());
        assertEquals(new ArrayList<>(s1), singletonList(11));
        assertEquals(new ArrayList<>(s02), asList(10, 12));

        // compare with lists
        assertEquals(e, emptyList());
        assertEquals(s1, singletonList(11));
        assertEquals(s02, asList(10, 12));

        // compare with sets
        assertEquals(e, emptySet());
        assertEquals(s1, singleton(11));
        assertEquals(s02, Sets.newHashSet(10, 12));

        //throw on modifiers
        for (ImmutableIndexedSubset<Integer> s : asList(e, s02, s1)) {
            expectThrows(UnsupportedOperationException.class, () -> s.add(10));
            expectThrows(UnsupportedOperationException.class, () -> s.addAll(asList(10, 11)));
            expectThrows(UnsupportedOperationException.class, () -> s.remove(10));
            expectThrows(UnsupportedOperationException.class, () -> s.removeAll(asList(10, 11)));
        }

        //copy from mutable is unaffected
        IndexedSubset<Integer> subset = set.subset(11);
        ImmutableIndexedSubset<Integer> copy = ImmutableIndexedSubset.copyOf(subset);
        assertEquals(copy, singleton(11));
        subset.add(12);
        assertEquals(copy, singleton(11));
        subset.remove(11);
        assertEquals(copy, singleton(11));
    }
}