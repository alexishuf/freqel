package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.BufferedResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class BufferedResultsTest implements TestContext {

    public static List<BufferedResults.Factory> factories = Arrays.asList(
            ListBufferedResults.FACTORY,
            HashDistinctResults.FACTORY
    );

    private static class MockResults extends CollectionResults {
        boolean isClosed = false;
        Integer readyCount = null;
        Boolean async = null;

        public MockResults(@Nonnull Collection<? extends Solution> collection,
                           @Nonnull Collection<String> varNames) {
            super(collection, varNames);
        }

        @Override
        public int getReadyCount() {
            return readyCount == null ? super.getReadyCount() : readyCount;
        }

        @Override
        public boolean isAsync() {
            return async == null ? super.isAsync() : async;
        }

        @Override
        public void close() {
            isClosed = true;
            super.close();

        }
    }

    private static @Nonnull MockResults createResults(int size) {
        ArraySolution.ValueFactory factory = ArraySolution.forVars(asList("x", "y"));
        List<Solution> list = new ArrayList<>();
        for (int i = 0; i < size; i++)
            list.add(factory.fromValues(new StdURI(EX+"x-"+i), new StdURI(EX+"y-"+i)));
        return new MockResults(list, factory.getVarNames());
    }

    private static @Nonnull List<Solution> consume(@Nonnull Results results) {
        List<Solution> list = new ArrayList<>();
        results.forEachRemaining(list::add);
        return list;
    }

    @DataProvider
    public Object[][] factoriesData() {
        return factories.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] sizeData() {
        return Stream.of(0, 1, 2, 4, 8, 16)
                .flatMap(i -> factories.stream().map(f -> new Object[]{i, f}))
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "factoriesData")
    public void testForwards(BufferedResults.Factory factory) {
        MockResults in = createResults(4);
        BufferedResults buffered = factory.create(in);
        assertEquals(buffered.getVarNames(), in.getVarNames());
        assertEquals(buffered.isAsync(), in.isAsync());
        assertEquals(buffered.getReadyCount(), in.getReadyCount());

        in.async = true;
        assertTrue(buffered.isAsync());

        in.readyCount = 23;
        assertEquals(buffered.getReadyCount(), 23);

        assertFalse(in.isClosed);

        buffered.close();
        assertTrue(in.isClosed);
    }

    @Test(dataProvider = "sizeData")
    public void testConsumeOnce(int size, BufferedResults.Factory factory) {
        MockResults in = createResults(size);
        try (BufferedResults buffered = factory.create(in)) {
            assertEquals(consume(buffered), in.getCollection());
            assertFalse(in.isClosed);
        }
        assertTrue(in.isClosed);
    }

    @Test(dataProvider = "sizeData")
    public void testConsumeAndReset(int size, BufferedResults.Factory factory) {
        MockResults in = createResults(size);
        try (BufferedResults buffered = factory.create(in)) {
            assertEquals(consume(buffered), in.getCollection());
            for (int i = 0; i < 4; i++) {
                assertEquals(consume(buffered), emptyList());
                buffered.reset();
                assertTrue(in.isClosed);
                List<Solution> consumed = consume(buffered);
                if (buffered.isOrdered())
                    assertEquals(consumed, in.getCollection());
                else
                    assertEquals(new HashSet<>(consumed), new HashSet<>(in.getCollection()));
                assertEquals(consume(buffered), emptyList());
            }
        }
        assertTrue(in.isClosed);
    }
}