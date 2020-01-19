package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.*;

public class UndirectedIrreflexiveArrayGraphTest {
    private static class IntegerGraph extends UndirectedIrreflexiveArrayGraph<Integer> {

        public IntegerGraph(@Nonnull List<Integer> nodes) {
            super(nodes);
        }
        public IntegerGraph(@Nonnull List<Integer> nodes, @Nonnull float[] weights) {
            super(nodes, weights);
        }
        public IntegerGraph() {
            super();
        }
        public IntegerGraph(int size) {
            this(createArray(size));
        }
        private static @Nonnull List<Integer> createArray(int size) {
            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < size; i++) list.add(i);
            return list;
        }
        @Override
        protected float weigh(@Nonnull Integer l, @Nonnull Integer r) {
            return Math.abs(l-r);
        }


    }
    @DataProvider
    public static Object[][] totalCellsData() {
        return new Object[][] {
                new Object[]{0, 0},
                new Object[]{1, 0},
                new Object[]{2, 1},
                new Object[]{3, 3},
                new Object[]{4, 6},
                new Object[]{6, 15},
        };
    }
    @Test(dataProvider = "totalCellsData")
    public void testTotalCells(int size, int expected) {
        assertEquals(UndirectedIrreflexiveArrayGraph.totalCells(size), expected);
    }

    @DataProvider
    public static Object[][] rowOffsetData() {
        return new Object[][] {
                new Object[]{1, 0, 0},
                new Object[]{2, 0, 0},
                new Object[]{3, 0, 0},
                new Object[]{4, 0, 0},
                new Object[]{2, 1, 1},
                new Object[]{3, 1, 2},
                new Object[]{4, 1, 3},
                new Object[]{5, 1, 4},
                new Object[]{3, 2, 3},
                new Object[]{4, 2, 5},
                new Object[]{5, 2, 7},
                new Object[]{6, 2, 9},
                new Object[]{4, 3, 6},
                new Object[]{5, 3, 9},
                new Object[]{6, 3, 12}
        };
    }

    @Test(dataProvider = "rowOffsetData")
    public void testRowOffset(int size, int idx, int expected) {
        IntegerGraph g = new IntegerGraph(size);
        assertEquals(g.rowOffset(idx), expected);
    }

    @Test
    public void testRemoveFromSingleton() {
        IntegerGraph g1 = new IntegerGraph(1);
        g1.removeAt(0);
        assertEquals(g1.indexOf(0), -1);

        IntegerGraph g2 = new IntegerGraph(1);
        g2.remove(0);
        assertEquals(g2.indexOf(0), -1);
    }

    @Test
    public void testRemoveFrom2() {
        IntegerGraph g1 = new IntegerGraph(2);
        g1.removeAt(0);
        assertEquals(g1.indexOf(0), -1);
        assertEquals(g1.indexOf(1), 0);

        IntegerGraph g2 = new IntegerGraph(2);
        g2.removeAt(1);
        assertEquals(g2.indexOf(0), 0);
        assertEquals(g2.indexOf(1), -1);
    }

    @Test
    public void testRemoveFirstFrom3() {
        IntegerGraph g = new IntegerGraph(3);
        g.removeAt(0);
        assertEquals(g.indexOf(0), -1);
        assertEquals(g.indexOf(1), 0);
        assertEquals(g.indexOf(2), 1);
        assertEquals(g.getWeight(0, 1), 1.0f);
    }

    @Test
    public void testRemoveSecondFrom3() {
        IntegerGraph g = new IntegerGraph(3);
        g.removeAt(1);
        assertEquals(g.indexOf(0), 0);
        assertEquals(g.indexOf(1), -1);
        assertEquals(g.indexOf(2), 1);
        assertEquals(g.getWeight(0, 1), 2.0f);
    }

    @DataProvider
    public static Object[][] sizesData() {
        return Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 11, 16, 23, 27, 32, 59, 64, 128)
                .map(i -> new Object[]{i})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAt(int size) {
        for (int removeIdx = 0; removeIdx < size; removeIdx++) {
            IntegerGraph g = new IntegerGraph(size);
            for (int i = 0; i < size; i++) { //sanity test
                for (int j = 0; j < size; j++) {
                    if (i != j)
                        assertEquals(g.getWeight(i, j), (float)Math.abs(i - j));
                }
            }
            g.removeAt(removeIdx);
            assertEquals(g.size(), size-1);

            // test indexOf()
            for (int i = 0; i < removeIdx; i++)
                assertEquals(g.indexOf(i), i);
            assertEquals(g.indexOf(removeIdx), -1);
            for (int i = removeIdx+1; i < size; i++)
                assertEquals(g.indexOf(i), i-1);

            // test getWeight()
            for (int i = 0; i < size; i++) {
                if (i == removeIdx) continue;
                int iIdx = g.indexOf(i);
                for (int j = 0; j < size; j++) {
                    if (j == i || j == removeIdx) continue;
                    int jIdx = g.indexOf(j);
                    assertEquals(g.getWeight(iIdx, jIdx), (float)Math.abs(i-j),
                            "size="+size+", removeIdx="+removeIdx+
                                     ", i="+i+", j="+j+", iIdx="+iIdx+", jIdx="+jIdx);
                }
            }
        }
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAllFromLeft(int size) {
        IntegerGraph g = new IntegerGraph(size);
        for (int start = 1; start < size; start++) {
            for (int i = start-1; i < size; i++) {
                for (int j = start-1; j < size; j++) {
                    if (i == j) continue;
                    int iIdx = g.indexOf(i), jIdx = g.indexOf(j);
                    assertEquals(g.weigh(iIdx, jIdx), (float)Math.abs(i-j));
                }
            }
            g.removeAt(0);
            for (int i = start; i < size; i++) {
                for (int j = start; j < size; j++) {
                    if (i == j) continue;
                    int iIdx = g.indexOf(i), jIdx = g.indexOf(j);
                    assertEquals(g.weigh(iIdx, jIdx), (float)Math.abs(i-j));
                }
            }
        }
    }

    @Test(dataProvider = "sizesData")
    public void testRemoveAllFromRight(int size) {
        IntegerGraph g = new IntegerGraph(size);
        for (int currSize = size; currSize > 0; currSize--) {
            for (int i = 0; i < currSize; i++) {
                for (int j = 0; j < currSize; j++) {
                    if (i == j) continue;
                    int iIdx = g.indexOf(i), jIdx = g.indexOf(j);
                    assertEquals(g.weigh(iIdx, jIdx), (float)Math.abs(i-j));
                }
            }
            g.removeAt(g.size()-1);
            for (int i = 0; i < currSize-1; i++) {
                for (int j = 0; j < currSize-1; j++) {
                    if (i == j) continue;
                    int iIdx = g.indexOf(i), jIdx = g.indexOf(j);
                    assertEquals(g.weigh(iIdx, jIdx), (float)Math.abs(i-j));
                }
            }
        }
    }

    @Test(dataProvider = "sizesData", invocationCount = 8, threadPoolSize = 4)
    public void testRandomRemove(int size) {
        IntegerGraph g = new IntegerGraph(size);
        int expectedSize = size;
        for (int removal = 0; removal < size; removal++) {
            int removeIdx = (int)Math.floor(Math.random() * g.size());
            g.removeAt(removeIdx);
            assertEquals(g.size(), --expectedSize);

            for (int i = 0; i < expectedSize; i++) {
                for (int j = 0; j < expectedSize; j++) {
                    if (i == j) continue;
                    int iNode = g.get(i), jNode = g.get(j);
                    assertEquals(g.getWeight(i, j), (float)Math.abs(iNode-jNode));
                }
            }
            assertEquals(g.isEmpty(), expectedSize==0);
        }
    }

    @Test
    public void testIsEmptyAndSize() {
        for (int size = 1; size < 64; size++) {
            IntegerGraph g = new IntegerGraph(size);
            assertFalse(g.isEmpty());
            assertEquals(g.size(), size);

            for (int i = 0; i < size; i++) {
                g.removeAt(0);
                assertEquals(g.size(), size-(i+1));
            }
            assertTrue(g.isEmpty());
        }
    }

    @Test(dataProvider = "sizesData")
    public void testGetNode(int size) {
        IntegerGraph g = new IntegerGraph(size);
        for (int i = 0; i < size; i++)
            assertEquals(g.get(i), Integer.valueOf(i));
    }

    @Test(dataProvider = "sizesData")
    public void testGetNodeAfterRemovingFirst(int size) {
        IntegerGraph g = new IntegerGraph(size);
        g.removeAt(0);
        for (int i = 0; i < size-1; i++)
            assertEquals(g.get(i), Integer.valueOf(i+1));
    }

    @Test(dataProvider = "sizesData")
    public void testGetNodes(int size) {
        IntegerGraph g = new IntegerGraph(size);
        ArrayList<Integer> expected = new ArrayList<>();
        for (int i = 0; i < size; i++)
            expected.add(i);
        assertEquals(g.getNodes(), expected);
    }

    @Test(dataProvider = "sizesData")
    public void testReplaceFromLeftToRight(int size) {
        IntegerGraph g = new IntegerGraph(size);
        for (int replaceIdx = 0; replaceIdx < size; replaceIdx++) {
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (i == j) continue;
                    int iValue = g.get(i), jValue = g.get(j);
                    assertEquals(g.getWeight(i, j), (float)Math.abs(iValue-jValue));
                }
            }

            ArrayList<Integer> expectedNodes = new ArrayList<>(g.getNodes());
            expectedNodes.set(replaceIdx, expectedNodes.get(replaceIdx)*-2);

            int old = g.get(replaceIdx);
            assertEquals(old, replaceIdx);
            g.replaceAt(replaceIdx, old*-2);
            assertEquals(g.getNodes(), expectedNodes);
            assertEquals(g.indexOf(old), old > 0 ? -1 : 0);
            assertEquals(g.indexOf(old*-2), replaceIdx);

            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (i == j) continue;
                    int iValue = g.get(i), jValue = g.get(j);
                    assertEquals(g.getWeight(i, j), (float)Math.abs(iValue-jValue));
                }
            }
        }
    }

    @Test(dataProvider = "sizesData")
    public void testReplaceFromRightToLeft(int size) {
        IntegerGraph g = new IntegerGraph(size);
        for (int replaceIdx = size-1; replaceIdx >= 0; replaceIdx--) {
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (i == j) continue;
                    int iValue = g.get(i), jValue = g.get(j);
                    assertEquals(g.getWeight(i, j), (float)Math.abs(iValue-jValue));
                }
            }

            ArrayList<Integer> expectedNodes = new ArrayList<>(g.getNodes());
            expectedNodes.set(replaceIdx, expectedNodes.get(replaceIdx)*-2);

            int old = g.get(replaceIdx);
            assertEquals(old, replaceIdx);

            g.replaceAt(replaceIdx, old*-2);
            assertEquals(g.getNodes(), expectedNodes);
            assertEquals(g.indexOf(old), old > 0 ? -1 : 0);
            assertEquals(g.indexOf(old*-2), replaceIdx);

            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (i == j) continue;
                    int iValue = g.get(i), jValue = g.get(j);
                    assertEquals(g.getWeight(i, j), (float)Math.abs(iValue-jValue));
                }
            }
        }
    }

    @Test
    public void testCreateEmpty() {
        IntegerGraph g = new IntegerGraph();
        assertEquals(g.size(), 0);
        assertTrue(g.isEmpty());
        assertEquals(g.getNodes(), Collections.emptyList());

        assertEquals(g.indexOf(0), -1);
        assertEquals(g.indexOf(23), -1);
        expectThrows(NoSuchElementException.class, () -> g.remove(0));
        expectThrows(NoSuchElementException.class, () -> g.replaceFirst(0, 23));
        expectThrows(IndexOutOfBoundsException.class, () -> g.removeAt(0));
        expectThrows(IndexOutOfBoundsException.class, () -> g.replaceAt(0, 23));
        expectThrows(IndexOutOfBoundsException.class, () -> g.get(0));
    }

    @DataProvider
    public static @Nonnull Object[][] subsetData() {
        List<Object[]> rows = new ArrayList<>();
        for (Integer size : asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 16, 17, 23, 32, 64, 128, 137)) {
            BitSet bitSet = new BitSet(size);
            List<Integer> indices = new ArrayList<>();

            int h = size / 2;
            for (List<Integer> bounds : asList(asList(0, h), asList(h, size), asList(0, size))) {
                indices.clear();  bitSet.clear();
                for (int i = bounds.get(0); i < bounds.get(1); i++) indices.add(i);
                indices.forEach(bitSet::set);
                rows.add(new Object[]{size, bitSet, indices});
            }
            for (Integer start : asList(0, 1)) {
                indices.clear();  bitSet.clear();
                for (int i = start; i < size; i += 2) indices.add(i);
                indices.forEach(bitSet::set);
                rows.add(new Object[]{size, bitSet, indices});
            }
        }
        return rows.toArray(new Object[0][]);
    }

    @Test(dataProvider = "subsetData")
    void testSubset(int size, BitSet subset, List<Integer> indices) {
        assertTrue(subset.cardinality() < size); //sanity
        assertEquals(subset.cardinality(), indices.size()); //sanity
        assertEquals(new HashSet<>(indices).size(), indices.size()); //sanity

        IntegerGraph g = new IntegerGraph(size);
        List<Integer> nodesSubset = new ArrayList<>();
        float[] weightsForSubset = g.getWeightsForSubset(subset, nodesSubset);

        IntegerGraph subgraph = new IntegerGraph(nodesSubset, weightsForSubset);
        assertEquals(subgraph.isEmpty(), indices.isEmpty());
        assertEquals(subgraph.size(), indices.size());
        assertEquals(subgraph.getNodes(), indices);

        for (int i = 0; i < indices.size(); i++)
            assertEquals(subgraph.indexOf(indices.get(i)), i, "i="+i);

        for (int i = 0; i < indices.size(); i++) {
            for (int j = 0; j < indices.size(); j++) {
                if (i == j) continue;
                Integer iValue = subgraph.get(i), jValue = subgraph.get(j);
                assertEquals(subgraph.getWeight(i, j), (float) Math.abs(iValue-jValue));
            }
        }
    }
}