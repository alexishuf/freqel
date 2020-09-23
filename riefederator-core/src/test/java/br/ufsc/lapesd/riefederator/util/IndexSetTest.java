package br.ufsc.lapesd.riefederator.util;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ImmFullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSetPartition;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
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

@Test(groups = {"fast"})
public class IndexSetTest implements TestContext {
    private static final int VALUES_BASE = 1000;

    private static @Nonnull IndexSet<Integer> set(int... values) {
        return FullIndexSet.fromDistinct(Arrays.stream(values).boxed().collect(toList()));
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
    public void testSize(IndexSet<Integer> set, int expected) {
        assertEquals(set.size(), expected);
        assertEquals(set.isEmpty(), expected == 0);
    }

    @Test(dataProvider = "sizesData")
    public void testContainsIndexOfAndGet(int size) {
        List<Integer> values = randomValues(size);
        IndexSet<Integer> set = FullIndexSet.from(values);
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
        IndexSet<Integer> set = FullIndexSet.from(values);

        for (int i = 0; i < 16; i++) {
            ArrayList<Integer> permutation = new ArrayList<>(values);
            Collections.shuffle(permutation);
            IndexSet<Integer> other = FullIndexSet.from(permutation);
            assertTrue(set.containsAll(other));
            assertTrue(other.containsAll(set));
            assertEquals(set, other);
            assertEquals(set.hashCode(), other.hashCode());
        }
    }

    @Test
    public void testNotEquals() {
        IndexSet<Integer> a = set(), b = set(1, 2), c = set(1), d = set(1, 2, 3), e = set(1, 3);

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
        IndexSet<Integer> copy1 = FullIndexSet.from(values);
        Collections.reverse(values);
        ArrayList<Integer> initial2 = new ArrayList<>(values);
        IndexSet<Integer> copy2 = FullIndexSet.fromDistinctCopy(values);
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
        IndexSet<Integer> set = new FullIndexSet<>(map, list);

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
        IndexSet<Integer> set = new FullIndexSet<>(map, list);

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
        IndexSet<Integer> set = FullIndexSet.fromDistinct(values);
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
        IndexSet<Integer> set = FullIndexSet.fromDistinct(values);

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
        IndexSet<Integer> set = FullIndexSet.fromDistinct(values);

        IndexSubset<Integer> empty = set.emptySubset();
        assertEquals(empty.getBitSet().cardinality(), 0);
        assertTrue(empty.isEmpty());
        //noinspection ConstantConditions
        assertEquals(empty.size(), 0);
        assertFalse(empty.iterator().hasNext());
        //noinspection SimplifyStreamApiCallChains
        assertEquals(empty.stream().collect(toSet()), emptySet());
        assertEquals(set.stream().filter(empty::contains).collect(toList()), emptyList());

        IndexSubset<Integer> all = set.fullSubset();
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

        IndexSubset<Integer> empty2 = set.emptySubset();
        assertEquals(empty, empty2);
        assertEquals(empty.hashCode(), empty2.hashCode());

        IndexSubset<Integer> all2 = set.fullSubset();
        assertEquals(all, all2);
        assertEquals(all.hashCode(), all2.hashCode());

        if (size > 0)
            assertNotEquals(empty, all);

        empty2.retainAll(all);
        assertEquals(empty2, empty);

        all2.retainAll(all);
        assertEquals(all2, all);

        all2.retainAll(empty2);
        assertEquals(all2, empty);
    }

    @Test
    public void testCopySubset() {
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> empty = set.emptySubset();
        IndexSubset<Integer> sub = empty.copy();
        sub.add(10);
        assertEquals(sub.size(), 1);
        assertEquals(empty.size(), 0);
        assertEquals(new ArrayList<>(sub), singletonList(10));
        assertEquals(new ArrayList<>(empty), emptyList());
    }

    @Test
    public void testIterateSubset() {
        IndexSet<Integer> set = set(10, 11, 12, 13, 14);
        IndexSubset<Integer> subset = set.subset(asList(13, 11));
        assertEquals(subset.size(), 2);

        Iterator<Integer> it = subset.iterator();
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(11));
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(13));
        assertFalse(it.hasNext());
        //noinspection ConstantConditions
        assertFalse(it.hasNext()); // test regression of an actual bug

        Iterator<Integer> it2 = subset.iterator();
        assertEquals(it2.next(), Integer.valueOf(11));
        assertEquals(it2.next(), Integer.valueOf(13));
        assertFalse(it2.hasNext());

        assertEquals(new ArrayList<>(subset), asList(11, 13));
        assertEquals(subset, Sets.newHashSet(11, 13));
    }

    @Test
    public void testRemoveFirstOfTwoInSubset() {
        IndexSet<Integer> set = set(10, 11, 12, 13, 14);
        IndexSubset<Integer> subset = set.subset(asList(13, 11));
        assertEquals(subset.size(), 2);

        Iterator<Integer> it = subset.iterator();
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(11));
        it.remove();
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(13));
        assertFalse(it.hasNext());
    }

    @Test
    public void testRemoveLastOfTwoInSubset() {
        IndexSet<Integer> set = set(10, 11, 12, 13, 14);
        IndexSubset<Integer> subset = set.subset(asList(13, 11));
        assertEquals(subset.size(), 2);

        Iterator<Integer> it = subset.iterator();
        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(11));

        assertTrue(it.hasNext());
        assertEquals(it.next(), Integer.valueOf(13));
        assertFalse(it.hasNext()); // should not disturb
        it.remove();
        assertFalse(it.hasNext());
    }

    @Test(dataProvider = "sizesData")
    public void testRemovePairsOfFullSubset(int size) {
        List<Integer> list = randomValues(size), expectedList = new ArrayList<>();
        for (int i = 1; i < size; i += 2)
            expectedList.add(list.get(i));
        IndexSet<Integer> set = FullIndexSet.from(list);
        IndexSubset<Integer> subset = set.fullSubset();
        assertEquals(new ArrayList<>(subset), new ArrayList<>(set));

        Iterator<Integer> it = subset.iterator();
        for (int i = 0; i < size; i++) {
            assertTrue(it.hasNext());
            assertEquals(it.next(), list.get(i));
            if (i % 2 == 0)
                it.remove();
        }
        assertEquals(new ArrayList<>(subset), expectedList);
    }

    @Test
    public void testIteratePairSubset() {
        IndexSet<Integer> set = set(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        for (int i = 0; i < set.size(); i++) {
            for (int j = 0; j < set.size(); j++) {
                IndexSubset<Integer> subset = set.subset(asList(10 + i, 10 + j));
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
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> sub = set.emptySubset();
        sub.add(11);
        assertEquals(new ArrayList<>(sub), singletonList(11));
        expectThrows(IllegalArgumentException.class, () -> sub.add(13));
    }

    @Test
    public void testSubsetRemoveExtraneous() {
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> sub = set.emptySubset();
        sub.addAll(asList(11, 12));

        assertFalse(sub.remove(Integer.valueOf(13)));
        assertEquals(new ArrayList<>(sub), asList(11, 12));
        assertFalse(sub.remove(Integer.valueOf(10)));
        assertEquals(new ArrayList<>(sub), asList(11, 12));
        assertTrue(sub.remove(Integer.valueOf(11)));
        assertEquals(new ArrayList<>(sub), singletonList(12));
    }

    @Test
    public void testSubsetContains() {
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> sub = set.emptySubset();
        sub.addAll(asList(10, 12));

        assertTrue(sub.contains(10));
        assertFalse(sub.contains(11));
        assertTrue(sub.contains(12));
        assertFalse(sub.contains(13));
    }

    @SuppressWarnings("SimplifiableAssertion") @Test
    public void testSubsetEqualsToArray() {
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> sub = set.fullSubset();

        assertEquals(sub, asList(10, 11, 12));
        assertTrue(sub.equals(asList(10, 11, 12)));
//        assertTrue(asList(10, 11, 12).equals(sub));

        //bad order is OK (but won't work by List.equals() implementation
//        assertEquals(sub, asList(12, 11, 10));
        assertTrue(sub.equals(asList(12, 11, 10)));
//        assertTrue(asList(12, 11, 10).equals(sub));

        sub.remove(Integer.valueOf(11));
//        assertEquals(sub, asList(12, 10));
        assertTrue(sub.equals(asList(12, 10)));
//        assertTrue(asList(12, 10).equals(sub));
    }

    @SuppressWarnings({"SimplifiedTestNGAssertion"})
    @Test
    public void testSubsetEqualsToSet() {
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> sub = set.fullSubset();

        assertEquals(sub, Sets.newHashSet(10, 11, 12));
        assertTrue(sub.equals(Sets.newHashSet(10, 11, 12)));
        assertTrue(Sets.newHashSet(10, 11, 12).equals(sub));

        //bad order is OK
        assertEquals(sub, Sets.newHashSet(12, 11, 10));
        assertTrue(sub.equals(Sets.newHashSet(12, 11, 10)));
        assertTrue(Sets.newHashSet(12, 11, 10).equals(sub));

        sub.remove(Integer.valueOf(11));
        assertEquals(sub, Sets.newHashSet(12, 10));
        assertTrue(sub.equals(Sets.newHashSet(12, 10)));
        assertTrue(Sets.newHashSet(12, 10).equals(sub));
    }

    @Test
    public void testFullSubsetContainsAll() {
        IndexSet<Integer> set = set(10, 11, 12);
        IndexSubset<Integer> full = set.fullSubset();
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

        IndexSet<Integer> set2 = set(11, 12, 13);
        assertTrue(full.containsAll(set2.subset(asList(11, 12))));
        assertTrue(full.containsAll(set2.subset(12)));
        assertTrue(full.containsAll(set2.emptySubset()));
        assertFalse(full.containsAll(set2.subset(13)));
        assertFalse(full.containsAll(set2.fullSubset()));
        assertFalse(full.containsAll(set2));
    }

    @Test
    public void testSubsetContainsAll() {
        IndexSet<Integer> set = set(10, 11, 12, 13), set2 = set(20, 21, 22, 11, 12, 14);
        IndexSubset<Integer> sub = set.subset(asList(11, 12));
        //noinspection SimplifiedTestNGAssertion
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
        IndexSet<Integer> a = set(10, 11, 12), b = set(11, 12, 13);
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
        IndexSet<Integer> a = set(11, 12, 13, 14, 15), b = set(15, 14, 13, 12, 11);
        IndexSet<Integer> c = set(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < a.size(); j++) {
                for (int k = 0; k < a.size(); k++) {
                    IndexSubset<Integer> aSubset = a.subset(asList(i, j, k));
                    IndexSubset<Integer> aSubset2 = a.subset(asList(j, i, k));
                    IndexSubset<Integer> bSubset = b.subset(asList(i, j, k));
                    IndexSubset<Integer> cSubset = c.subset(asList(i, j, k));

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
        IndexSet<Integer> set = set(10, 11, 12, 13, 14, 15);
        IndexSubset<Integer> full = set.fullSubset();
        IndexSubset<Integer> emptySubset = set.emptySubset();

        IndexSubset<Integer> s0 = emptySubset.copy();
        s0.add(10);
        assertNotEquals(s0, emptySubset);
        assertEquals(new ArrayList<>(s0), singletonList(10));
        s0.retainAll(full);
        assertEquals(new ArrayList<>(s0), singletonList(10));
        s0.retainAll(emptySubset);
        assertEquals(s0, emptySubset);

        IndexSubset<Integer> s13 = emptySubset.copy();
        s13.add(11);
        s13.add(13);
        s0.add(10);
        assertEquals(s0.createIntersection(s13), emptySubset);

        IndexSubset<Integer> s013 = s0.createUnion(s13);
        IndexSubset<Integer> s013_ = emptySubset.copy();
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

        IndexSubset<Integer> s35 = emptySubset.copy();
        s35.addAll(asList(13, 15));
        assertEquals(new ArrayList<>(s35), asList(13, 15));
        assertEquals(new ArrayList<>(s35.createIntersection(s013)), singletonList(13));
        assertEquals(new ArrayList<>(s35.createUnion(s013)), asList(10, 11, 13, 15));
    }

    @Test
    public void testSetDifference() {
        IndexSet<Integer> set = set(10, 11, 12, 13, 14, 15);

        assertEquals(set.emptySubset().createDifference(set.fullSubset()), set.emptySubset());
        assertEquals(set.emptySubset().createDifference(set.subset(10)), set.emptySubset());
        assertEquals(set.fullSubset().createDifference(set.subset(11)),
                     set.subset(asList(10, 12, 13, 14, 15)));
        assertEquals(set.fullSubset().createDifference(set.subset(asList(11, 12, 13))),
                     set.subset(asList(10, 14, 15)));
        assertEquals(set.fullSubset().createDifference(set.subset(asList(11, 13, 15))),
                     set.subset(asList(10, 12, 14)));

        IndexSubset<Integer> subset = set.fullSubset();
        assertTrue(subset.removeAll(set.subset(asList(10, 12, 14))));
        assertEquals(subset, set.subset(asList(11, 13, 15)));

        subset = set.fullSubset();
        assertTrue(subset.removeAll(set.subset(asList(11, 13, 15))));
        assertEquals(subset, set.subset(asList(10, 12, 14)));

        subset = set.subset(asList(10, 11, 12));
        assertTrue(subset.removeAll(set.subset(asList(11, 14, 15))));
        assertEquals(subset, set.subset(asList(10, 12)));

        subset = set.subset(asList(11, 13, 15));
        assertTrue(subset.removeAll(asList(10, 11, 13)));
        assertEquals(subset, set.subset(15));

        subset = set.subset(asList(10, 12, 14));
        assertTrue(subset.removeAll(set.fullSubset()));
        assertEquals(subset, set.emptySubset());

        subset = set.subset(asList(11, 13, 15));
        assertTrue(subset.removeAll(set.subset(asList(15, 13, 11))));
        assertEquals(subset, set.emptySubset());
    }


    @Test
    public void testUniversalEmpty() {
        FullIndexSet<Integer> e0 = ImmFullIndexSet.empty();
        IndexSubset<Integer> e1 = e0.emptySubset();
        ImmIndexSubset<Integer> e2 = e0.immutableEmptySubset();
        IndexSet<Integer> set = set(10, 11);
        IndexSubset<Integer> e3 = set.emptySubset();
        ImmIndexSubset<Integer> e4 = set.immutableEmptySubset();

        assertEquals(e1, e2);
        assertEquals(e1, e3);
        assertEquals(e1, e4);
        assertEquals(e2, e3);
        assertEquals(e2, e4);
        assertEquals(e3, e4);
    }

    @Test
    public void testImmutableSubset() {
        IndexSet<Integer> set = set(10, 11, 12);
        ImmIndexSubset<Integer> e = set.immutableEmptySubset();
        ImmIndexSubset<Integer> s1 = set.immutableSubset(11);
        ImmIndexSubset<Integer> s02 = set.immutableSubset(asList(10, 12));

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
        for (ImmIndexSubset<Integer> s : asList(e, s02, s1)) {
            expectThrows(UnsupportedOperationException.class, () -> s.add(10));
            expectThrows(UnsupportedOperationException.class, () -> s.addAll(asList(10, 11)));
            expectThrows(UnsupportedOperationException.class, () -> s.remove(10));
            expectThrows(UnsupportedOperationException.class, () -> s.removeAll(asList(10, 11)));
        }

        //copy from mutable is unaffected
        IndexSubset<Integer> subset = set.subset(11);
        ImmIndexSubset<Integer> copy = subset.immutableCopy();
        assertEquals(copy, singleton(11));
        subset.add(12);
        assertEquals(copy, singleton(11));
        subset.remove(Integer.valueOf(11));
        assertEquals(copy, singleton(11));
    }

    @SuppressWarnings("SimplifiableAssertion") @Test
    public void testEqualsWithSingleton() {
        IndexSet<Triple> a = FullIndexSet.fromDistinct(asList(new Triple(Alice, age, u)));
        Set<Triple> b = singleton(new Triple(Alice, age, u));
        assertEquals(a, b);
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

    @Test
    public void testHashCodeConsistencySingleton() {
        IndexSet<Integer> a = FullIndexSet.fromDistinct(singletonList(23));
        Set<Integer> aSingleton = singleton(23);
        HashSet<Integer> aStd = Sets.newHashSet(23);

        assertEquals(a.hashCode(), aSingleton.hashCode());
        assertEquals(a.hashCode(), aStd.hashCode());
    }

    @Test
    public void testHashCodeOrderNeutral() {
        IndexSet<Integer> a = FullIndexSet.fromDistinct(asList(1, 2, 3, 4));
        IndexSet<Integer> b = FullIndexSet.fromDistinct(asList(2, 3, 4, 1));
        IndexSet<Integer> c = FullIndexSet.fromDistinct(asList(3, 4, 1, 2));
        IndexSet<Integer> d = FullIndexSet.fromDistinct(asList(4, 1, 2, 3));
        IndexSet<Integer> e = FullIndexSet.fromDistinct(asList(2, 1, 4, 3));
        IndexSet<Integer> f = FullIndexSet.fromDistinct(asList(3, 1, 4, 2));

        List<Integer> codes = asList(a.hashCode(), b.hashCode(), c.hashCode(), d.hashCode(),
                                     e.hashCode(), f.hashCode());
        assertEquals(new HashSet<>(codes).size(), 1);
    }

    @Test
    public void testHashCodeStdConsistent() {
        IndexSet<Integer> a1 = FullIndexSet.fromDistinct(asList(1, 2, 3, 4));
        HashSet<Integer> a2 = Sets.newHashSet(1, 2, 3, 4);
        TreeSet<Integer> a3 = new TreeSet<>();
        a3.addAll(asList(1, 2, 3, 4));
        assertEquals(a1.hashCode(), a2.hashCode());
        assertEquals(a1.hashCode(), a3.hashCode());

        assertEquals(a1, a2);
        assertEquals(a1, a3);
    }

    @SuppressWarnings("SimplifiableAssertion") @Test
    public void testEqualsRegression() {
        IndexSet<Triple> a1 = FullIndexSet.fromDistinct(asList(new Triple(Alice, age, u)));
        IndexSet<Triple> a2 = FullIndexSet.fromDistinct(asList(new Triple(Alice, age, v)));
        Set<Triple> b1 = singleton(new Triple(Alice, age, u));
        Set<Triple> b2 = singleton(new Triple(Alice, age, v));
        HashSet<Set<Triple>> a = new HashSet<>();
        a.add(a1);
        a.add(a2);
        assertEquals(a.size(), 2);
        assertTrue(a.contains(a1));
        assertTrue(a.contains(a2));

        //noinspection unchecked
        HashSet<Set<Triple>> b = Sets.newHashSet(b1, b2);
        assertEquals(b.size(), 2);
        assertTrue(b.contains(b1));
        assertTrue(b.contains(b2));

        assertTrue(a.containsAll(b));
        assertTrue(b.containsAll(a));

        assertEquals(a, b);
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

    private static class Thing {
        private int id;
        public Thing(int id) {
            this.id = id;
        }
        @Override public boolean equals(Object o) {
            return o instanceof Thing && id == ((Thing)o).id;
        }
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }

    @Test(dataProvider = "sizesData")
    public void testFromRefDistinct(int size) {
        List<Thing> in = new ArrayList<>(), ex = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            in.add(new Thing(i));
            ex.add(in.get(in.size()-1));
        }
        IndexSet<Thing> set = RefIndexSet.fromRefDistinct(in);
        assertEquals(in, ex); //in not modified
        assertTrue(set.containsAll(in));
        assertTrue(in.containsAll(set));
        assertEquals(in, new ArrayList<>(set)); //iteration order preserved
        for (Thing thing : ex)
            assertEquals(set.indexOf(thing), in.indexOf(thing)); // indexOf corresponds to iteration
        for (int i = 0; i < size; i++) { //do not contain equal but distinct
            Thing distinct = new Thing(i);
            assertFalse(set.contains(distinct));
            assertEquals(set.indexOf(distinct), -1);
            assertTrue(in.contains(distinct)); //ArrayList honors equals()
        }
    }

    @Test(dataProvider = "sizesData")
    public void testFromRefDistinctDuplicates(int size) {
        List<Thing> in = new ArrayList<>(), ex = new ArrayList<>();
        for (int i = 0; i < 2*size; i++) {
            in.add(new Thing(i % size));
            ex.add(in.get(in.size()-1));
        }
        IndexSet<Thing> set = RefIndexSet.fromRefDistinct(in);
        assertEquals(set.size(), 2*size);
        assertEquals(new ArrayList<>(set), ex);
        for (int i = 0, exSize = ex.size(); i < exSize; i++) {
            Thing thing = ex.get(i);
            assertTrue(set.contains(thing));
            assertEquals(set.indexOf(thing), i);
        }
        set.containsAll(ex);
        assertEquals(in, ex);
    }

    @Test(dataProvider = "sizesData")
    public void testFromRefDistinctCopy(int size) {
        List<Thing> in = new ArrayList<>(), ex = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            in.add(new Thing(i));
            ex.add(in.get(in.size()-1));
        }
        IndexSet<Thing> set = RefIndexSet.fromRefDistinctCopy(in);
        assertEquals(new ArrayList<>(set), ex);
        Collections.reverse(in);
        assertEquals(new ArrayList<>(set), ex);
        for (Thing thing : ex)
            assertEquals(set.indexOf(thing), ex.indexOf(thing));

        in.clear();
        for (Thing thing : ex)
            assertTrue(set.contains(thing));
        assertTrue(set.containsAll(ex));

        for (int i = 0; i < size; i++) {
            Thing copy = new Thing(i);
            assertFalse(set.contains(copy));
            assertEquals(set.indexOf(copy), -1);
        }
    }

    @Test(dataProvider = "sizesData")
    public void testFullPartition(int size) {
        List<Integer> in = randomValues(size);
        ArrayList<Integer> expected = new ArrayList<>(in);
        IndexSet<Integer> set = FullIndexSet.fromDistinct(in);
        assertEquals(new ArrayList<>(set), expected);

        IndexSetPartition<Integer> partition = IndexSetPartition.of(set, 0, in.size());
        assertEquals(partition.size(), expected.size());
        assertEquals(new ArrayList<>(partition), expected);
        //noinspection SimplifiableAssertion
        assertTrue(partition.equals(set));
        //noinspection SimplifiableAssertion
        assertTrue(partition.equals(expected));
        assertEquals(partition.hashCode(), new HashSet<>(expected).hashCode());
    }

    @Test(dataProvider = "sizesData")
    public void testHalfPartition(int size) {
        if (size < 2) return;

        List<Integer> in = randomValues(size);
        ArrayList<Integer> expected = new ArrayList<>(in.size()/2);
        for (int i = 0; i < size / 2; i++)
            expected.add(in.get(i));
        IndexSet<Integer> set = FullIndexSet.fromDistinct(in);

        IndexSetPartition<Integer> partition = IndexSetPartition.of(set, 0, in.size() / 2);
        assertEquals(partition.size(), expected.size());
        assertEquals(new ArrayList<>(partition), expected);
        //noinspection SimplifiableAssertion
        assertTrue(partition.equals(expected));
        assertEquals(partition.hashCode(), new HashSet<>(expected).hashCode());
    }

    @Test
    public void testAddToIndexedSet() {
        FullIndexSet<Integer> set = new FullIndexSet<>(4);
        assertTrue(set.add(23));
        IndexSubset<Integer> oldFull = set.fullSubset();
        assertTrue(set.add(2));
        assertFalse(set.add(23));

        assertTrue(set.contains(23));
        assertFalse(set.contains(0));
        assertTrue(set.containsAll(asList(23, 2)));

        assertEquals(oldFull.size(), 1);
        assertTrue(set.containsAll(oldFull));
        assertTrue(set.containsAny(oldFull));
        assertFalse(oldFull.containsAll(set));
        assertTrue(oldFull.containsAny(set));

        assertEquals(set.indexOf(23), 0);
        assertEquals(set.indexOf(2), 1);
        assertEquals(set.indexOf(0), -1);
        assertEquals(set.indexOf(5), -1);

        expectThrows(UnsupportedOperationException.class, () -> set.remove(0));
        expectThrows(UnsupportedOperationException.class, () -> set.remove(Integer.valueOf(2)));
        expectThrows(UnsupportedOperationException.class, () -> set.set(0, 5));
        expectThrows(UnsupportedOperationException.class, () -> set.add(1, 5));
        expectThrows(UnsupportedOperationException.class, () -> set.addAll(1, asList(5, 7)));
        expectThrows(UnsupportedOperationException.class, () -> set.retainAll(asList(5, 7)));

        assertTrue(set.containsAll(set.fullSubset()));
        assertTrue(set.containsAll(set.emptySubset()));
        assertTrue(set.containsAll(set.immutableFullSubset()));
        assertTrue(set.containsAll(set.immutableEmptySubset()));

        assertEquals(oldFull.createIntersection(set), singleton(23));
        IndexSubset<Integer> subset = oldFull.copy();
        assertTrue(subset.add(2));
        assertFalse(oldFull.contains(2));
        assertEquals(subset.createIntersection(set), set);
    }

    @Test
    public void testIterateFullSubset() {
        IndexSubset<Integer> s0 = new FullIndexSet<Integer>(0).fullSubset();
        IndexSubset<Integer> s1 = FullIndexSet.newIndexedSet(10).fullSubset();
        IndexSubset<Integer> s2 = FullIndexSet.newIndexedSet(10, 11).fullSubset();
        IndexSubset<Integer> s3 = FullIndexSet.newIndexedSet(10, 11, 12).fullSubset();

        assertFalse(s0.iterator().hasNext());

        Iterator<Integer> i1 = s1.iterator();
        assertTrue(i1.hasNext());
        assertEquals(i1.next(), Integer.valueOf(10));
        assertFalse(i1.hasNext());

        Iterator<Integer> i2 = s2.iterator();
        assertTrue(i2.hasNext());
        assertEquals(i2.next(), Integer.valueOf(10));
        assertTrue(i2.hasNext());
        assertEquals(i2.next(), Integer.valueOf(11));
        assertFalse(i2.hasNext());

        Iterator<Integer> i3 = s3.iterator();
        assertTrue(i3.hasNext());
        assertEquals(i3.next(), Integer.valueOf(10));
        assertTrue(i3.hasNext());
        assertEquals(i3.next(), Integer.valueOf(11));
        assertTrue(i3.hasNext());
        assertEquals(i3.next(), Integer.valueOf(12));
        assertFalse(i3.hasNext());
    }


    @Test
    public void testIterateImmutableFullSubset() {
        IndexSubset<Integer> s0 = new FullIndexSet<Integer>(0).immutableFullSubset();
        IndexSubset<Integer> s1 = FullIndexSet.newIndexedSet(10).immutableFullSubset();
        IndexSubset<Integer> s2 = FullIndexSet.newIndexedSet(10, 11).immutableFullSubset();
        IndexSubset<Integer> s3 = FullIndexSet.newIndexedSet(10, 11, 12).immutableFullSubset();

        assertFalse(s0.iterator().hasNext());

        Iterator<Integer> i1 = s1.iterator();
        assertTrue(i1.hasNext());
        assertEquals(i1.next(), Integer.valueOf(10));
        assertFalse(i1.hasNext());

        Iterator<Integer> i2 = s2.iterator();
        assertTrue(i2.hasNext());
        assertEquals(i2.next(), Integer.valueOf(10));
        assertTrue(i2.hasNext());
        assertEquals(i2.next(), Integer.valueOf(11));
        assertFalse(i2.hasNext());

        Iterator<Integer> i3 = s3.iterator();
        assertTrue(i3.hasNext());
        assertEquals(i3.next(), Integer.valueOf(10));
        assertTrue(i3.hasNext());
        assertEquals(i3.next(), Integer.valueOf(11));
        assertTrue(i3.hasNext());
        assertEquals(i3.next(), Integer.valueOf(12));
        assertFalse(i3.hasNext());
    }

    @Test(dataProvider = "sizesData")
    public void testLowerPartition(int size) {
        FullIndexSet<Integer> set = new FullIndexSet<>(0);
        List<Integer> expectedPartition = new ArrayList<>(), expectedSet = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            set.add(i);
            expectedSet.add(i);
            if (i < size/2)
                expectedPartition.add(i);
        }
        assertEquals(set, expectedSet);

        IndexSetPartition<Integer> partition;
        partition = IndexSetPartition.of(set, 0, size / 2);
        assertEquals(partition.size(), size/2);
        assertEquals(partition.getBegin(), 0);
        assertEquals(partition.getEnd(), size/2);
        assertEquals(partition, expectedPartition);

        assertEquals(partition.fullSubset(), expectedPartition);
        assertEquals(partition.immutableFullSubset(), expectedPartition);

        IndexSubset<Integer> subset = partition.emptySubset();
        List<Integer> subsetExpected = new ArrayList<>();
        for (int i = 0; i < size / 2; i++) {
            assertTrue(subset.add(i));
            subsetExpected.add(i);
            assertEquals(subset.size(), i+1);
            assertEquals(subset.size(), subsetExpected.size());
            assertEquals(subset, subsetExpected);
        }
        for (int i = 0; i < size / 2; i++) {
            assertFalse(subset.add(i)); // no effect
            assertEquals(subset, subsetExpected);
        }
        expectThrows(IllegalArgumentException.class, () -> subset.add(size/2));
    }

    @Test(dataProvider = "sizesData")
    public void testSubsetOfPartition(int size) {
        List<Integer> partitionExpected = new ArrayList<>();
        int pSize = size / 2;
        IndexSet<Integer> full = new FullIndexSet<>(size);
        for (int i = 0; i < size; i++) {
            full.add(i);
            if (i < pSize)
                partitionExpected.add(i);
        }
        IndexSetPartition<Integer> partition = IndexSetPartition.of(full, 0, pSize);

        assertEquals(partition.size(), pSize);
        assertEquals(partition.size(), partitionExpected.size());
        assertEquals(partition, partitionExpected);
        assertTrue(partition.containsAll(partitionExpected));
        assertEquals(partition.containsAny(partitionExpected), pSize > 0);
        assertTrue(partitionExpected.containsAll(partition));
        for (Integer v : partitionExpected)
            assertEquals(partition.indexOf(v), (int)v);
        for (int i = pSize; i < size; i++) {
            assertFalse(partition.contains(i));
            assertEquals(partition.indexOf(i), -1);
        }

        IndexSubset<Integer> fullSubset = partition.fullSubset();
        assertEquals(fullSubset.size(), pSize);
        assertEquals(fullSubset, partition);
        assertEquals(fullSubset, partitionExpected);
        assertTrue(partitionExpected.containsAll(fullSubset));
        for (int i = 0; i < pSize; i++)
            assertEquals(fullSubset.hasIndex(i, partition), i < pSize);
        expectThrows(IndexOutOfBoundsException.class, () -> fullSubset.hasIndex(pSize, partition));
        if (pSize > 0)
            expectThrows(AssertionError.class, () -> fullSubset.hasIndex(0, full));

        IndexSubset<Integer> emptySubset = partition.emptySubset();
        assertTrue(emptySubset.isEmpty());
        assertEquals(emptySubset, emptySet());
        assertTrue(partition.containsAll(emptySubset));
        assertFalse(partition.containsAny(emptySubset));

        List<Integer> evenExpected = new ArrayList<>();
        IndexSubset<Integer> even = partition.emptySubset().copy();
        for (int i = 0; i < pSize; i += 2) {
            assertTrue(even.add(i));
            evenExpected.add(i);
        }
        for (int i = 0; i < pSize; i += 2)
            assertFalse(even.add(i));
        expectThrows(IllegalArgumentException.class, () -> even.add(-1));
        expectThrows(IllegalArgumentException.class, () -> even.add(pSize));
        expectThrows(IllegalArgumentException.class, () -> even.add(size));
        assertEquals(emptySubset, emptySet());
        assertEquals(even, evenExpected);

        for (int i = 1; i < size; i += 2)
            assertFalse(even.contains(Integer.valueOf(i)));
    }

    @Test(dataProvider = "sizesData")
    public void testRightSubsetOfPartition(int size) {
        List<Integer> partitionExpected = new ArrayList<>();
        int pStart = size / 2;
        int pSize = size - pStart;
        IndexSet<Integer> full = new FullIndexSet<>(size);
        for (int i = 0; i < size; i++) {
            full.add(i);
            if (i >= pStart)
                partitionExpected.add(i);
        }

        IndexSetPartition<Integer> partition = IndexSetPartition.of(full, pStart, pStart+pSize);

        assertEquals(partition.size(), pSize);
        assertEquals(partition.size(), partitionExpected.size());
        assertEquals(partition, partitionExpected);
        assertTrue(partition.containsAll(partitionExpected));
        assertEquals(partition.containsAny(partitionExpected), pSize > 0);
        assertTrue(partitionExpected.containsAll(partition));
        for (Integer v : partitionExpected)
            assertEquals(partition.indexOf(v), v - pStart);
        for (int i = 0; i < pStart; i++) {
            assertFalse(partition.contains(i));
            assertEquals(partition.indexOf(i), -1);
        }

        IndexSubset<Integer> fullSubset = partition.fullSubset();
        assertEquals(fullSubset.size(), pSize);
        assertEquals(fullSubset, partition);
        assertEquals(fullSubset, partitionExpected);
        assertTrue(partitionExpected.containsAll(fullSubset));
        for (int i = 0; i < pSize; i++)
            assertTrue(fullSubset.hasIndex(i, partition));

        IndexSubset<Integer> emptySubset = partition.emptySubset();
        assertTrue(emptySubset.isEmpty());
        assertEquals(emptySubset, emptySet());
        assertTrue(partition.containsAll(emptySubset));
        assertFalse(partition.containsAny(emptySubset));

        List<Integer> evenExpected = new ArrayList<>();
        IndexSubset<Integer> even = partition.emptySubset().copy();
        for (int i = pStart; i < size; i += 2) {
            assertTrue(even.add(i));
            evenExpected.add(i);
        }
        for (int i = 0; i < pSize; i += 2)
            assertFalse(even.add(pStart+i));
        expectThrows(IllegalArgumentException.class, () -> even.add(size));
        expectThrows(IllegalArgumentException.class, () -> even.add(pStart-1));
        assertEquals(emptySubset, emptySet());
        assertEquals(even, evenExpected);
        for (int i = 0; i < pSize; i++)
            assertEquals(even.hasIndex(i, partition), i % 2 == 0);

        for (int i = pStart+1; i < size; i += 2)
            assertFalse(even.contains(Integer.valueOf(i)));
        for (int i = pStart-1; i >= 0; i -= 2)
            assertFalse(even.contains(Integer.valueOf(i)));
    }
}