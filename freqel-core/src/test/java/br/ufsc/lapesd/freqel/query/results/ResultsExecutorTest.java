package br.ufsc.lapesd.freqel.query.results;

import br.ufsc.lapesd.freqel.NamedSupplier;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.query.results.impl.SequentialResultsExecutor;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.*;

public class ResultsExecutorTest implements TestContext {
    private static final List<Class<? extends ResultsExecutor>> classes
            = asList(BufferedResultsExecutor.class, SequentialResultsExecutor.class);

    private static final List<NamedSupplier<? extends ResultsExecutor>> suppliers =
            Arrays.asList(
                    new NamedSupplier<>("BufferedAsyncResultsExecutor+singleThread",
                            () -> new BufferedResultsExecutor(Executors.newSingleThreadExecutor(), 2)),
                    new NamedSupplier<>("BufferedAsyncResultsExecutor+singleThread",
                            () -> new BufferedResultsExecutor(Executors.newSingleThreadExecutor(), 1))
            );

    @DataProvider
    public static @Nonnull Object[][] suppliersData() {
        return Stream.concat(suppliers.stream(), classes.stream().map(NamedSupplier::new))
                .map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "suppliersData", groups = {"fast"})
    public void testNoInput(Supplier<ResultsExecutor> supplier) {
        ResultsExecutor executor = supplier.get();
        try (Results results = executor.async(emptyList(), null)) {
            assertFalse(results.hasNext());
        }
    }

    @Test(dataProvider = "suppliersData", groups = {"fast"})
    public void testSingleEmptyInput(Supplier<ResultsExecutor> supplier) {
        ResultsExecutor executor = supplier.get();
        try (Results r = executor.async(CollectionResults.empty(singleton("x")))) {
            assertFalse(r.hasNext());
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testParallelSingleEmptyInput(Supplier<ResultsExecutor> supplier)
            throws ExecutionException, InterruptedException {
        ExecutorService outer = Executors.newCachedThreadPool();
        ResultsExecutor e = supplier.get();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 200 * Runtime.getRuntime().availableProcessors(); i++) {
            Future<?> future = outer.submit(() -> {
                try (Results r = e.async(CollectionResults.empty(singleton("x")))) {
                    assertFalse(r.hasNext());
                }
            });
            futures.add(future);
        }
        for (Future<?> future : futures)
            future.get(); //will throw ExecutionException if failed
        outer.shutdown();
        assertTrue(outer.awaitTermination(1, TimeUnit.SECONDS));
    }

    private static class MockResults extends CollectionResults{
        boolean closed = false;
        private final String name;

        public MockResults(@Nonnull Collection<Solution> collection,
                           @Nonnull Collection<String> varNames, @Nonnull String name) {
            super(collection, varNames);
            this.name = name;
        }

        @Override
        public String toString() {
            return "MockResults{"+name+"}";
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private @Nonnull List<List<Solution>> generateInputLists(int columns, int rows) {
        List<List<Solution>> list = new ArrayList<>();
        for (int i = 0; i < columns; i++) {
            List<Solution> solutions = new ArrayList<>();
            for (int j = 0; j < rows; j++)
                solutions.add(MapSolution.build(x, lit(i*rows + j)));
            list.add(solutions);
        }
        return list;
    }

    private @Nonnull List<MockResults> generateInput(List<List<Solution>> lists) {
        List<MockResults> list = new ArrayList<>();
        for (int i = 0; i < lists.size(); i++)
            list.add(new MockResults(lists.get(i), singleton("x"), "col="+i));
        return list;
    }

    private @Nonnull BitSet expected(int columns, int rows) {
        int size = columns * rows;
        BitSet bitSet = new BitSet(size);
        bitSet.set(0, size);
        return bitSet;
    }

    private void store(@Nonnull BitSet bitSet, @Nonnull Solution solution) {
        Term term = solution.get(x);
        assertNotNull(term);
        String string = term.asLiteral().getLexicalForm();
        int idx = parseInt(string);
        assertTrue(idx >= 0);
        bitSet.set(idx);
    }

    @DataProvider
    public static @Nonnull Object[][] inputData() {
        return Arrays.stream(suppliersData()).map(a -> a[0]).flatMap(s ->
            Stream.of(
                    asList(s, 1, 10),
                    asList(s, 2, 3),
                    asList(s, 2, 1000),
                    asList(s, 10, 1000),
                    asList(s, 100, 1000),
                    asList(s, 1000, 1),
                    asList(s, 1000, 10),
                    asList(s, 1000, 0)
            ).map(List::toArray)
        ).toArray(Object[][]::new);

    }

    @Test(dataProvider = "inputData")
    public void testConsume(Supplier<ResultsExecutor> supplier,
                            int columns, int rows) {
        ResultsExecutor executor = supplier.get();
        List<MockResults> inputs = generateInput(generateInputLists(columns, rows));
        BitSet actual = new BitSet(columns);

        Results results = executor.async(inputs, null);
        results.forEachRemainingThenClose( s -> store(actual, s));

        assertTrue(inputs.stream().allMatch(MockResults::isClosed));
        assertEquals(actual, expected(columns, rows));
    }

    @Test(dataProvider = "suppliersData")
    public void testConsumeProjecting(Supplier<ResultsExecutor> supplier) {
        ResultsExecutor executor = supplier.get();
        MockResults inXZ = new MockResults(singletonList(
                MapSolution.builder().put(x, lit(1)).put(z, lit(2)).build()
        ), Sets.newHashSet("x", "z"), "inXZ");
        MockResults inXY = new MockResults(asList(
                MapSolution.builder().put(x, lit(3)).put(y, lit(4)).build(),
                MapSolution.builder().put(x, lit(5)).put(y, lit(6)).build()
        ), Sets.newHashSet("x", "y"), "inXY");

        List<MockResults> inputs = asList(inXZ, inXY);
        Results results = executor.async(inputs, singletonList("x"));
        Set<Solution> actual = new HashSet<>();
        results.forEachRemainingThenClose(actual::add);

        assertTrue(inputs.stream().allMatch(MockResults::isClosed));
        assertEquals(actual, Sets.newHashSet(
                MapSolution.build(x, lit(1)),
                MapSolution.build(x, lit(3)),
                MapSolution.build(x, lit(5))
        ));
    }

    @Test(dataProvider = "inputData")
    public void testParallelConsume(Supplier<ResultsExecutor> supplier,
                            int columns, int rows) throws Exception {
        ExecutorService outer = Executors.newCachedThreadPool();
        ResultsExecutor executor = supplier.get();
        List<List<Solution>> inputLists = generateInputLists(columns, rows);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 10*Runtime.getRuntime().availableProcessors(); i++) {
            futures.add(outer.submit(() -> {
                List<MockResults> inputs = generateInput(inputLists);
                BitSet actual = new BitSet(columns);

                Results results = executor.async(inputs, null);
                results.forEachRemainingThenClose( s -> store(actual, s));

                assertTrue(inputs.stream().allMatch(MockResults::isClosed));
                assertEquals(actual, expected(columns, rows));
            }));
        }

        for (Future<?> future : futures)
            future.get(); //thros AssertionError's
        outer.shutdown();
        assertTrue(outer.awaitTermination(1, SECONDS));
    }

    @Test(dataProvider = "inputData")
    public void testParallelConsumePolling(Supplier<ResultsExecutor> supplier,
                                           int columns, int rows) throws Exception {
        ExecutorService outer = Executors.newCachedThreadPool();
        ResultsExecutor executor = supplier.get();
        List<List<Solution>> inputLists = generateInputLists(columns, rows);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 2*Runtime.getRuntime().availableProcessors(); i++) {
            futures.add(outer.submit(() -> {
                List<MockResults> inputs = generateInput(inputLists);
                BitSet actual = new BitSet(columns);

                Results results = executor.async(inputs, null);
                int total = inputs.stream().map(r -> r.getCollection().size())
                                           .reduce(Integer::sum).orElse(0);
                for (int j = 0; j < total; j++) {
                    //noinspection StatementWithEmptyBody
                    while (!results.hasNext(0)) {/* poll */}
                    assertTrue(results.hasNext(0));
                    store(actual, results.next());
                }
                assertFalse(results.hasNext());
                results.close();

                assertTrue(inputs.stream().allMatch(MockResults::isClosed));
                assertEquals(actual, expected(columns, rows));
            }));
        }

        for (Future<?> future : futures)
            future.get(); //throws AssertionError's
        outer.shutdown();
        assertTrue(outer.awaitTermination(1, SECONDS));
    }

    @Test(dataProvider = "inputData")
    public void testParallelClose(Supplier<ResultsExecutor> supplier,
                                    int columns, int rows) throws Exception {
        ExecutorService outer = Executors.newCachedThreadPool();
        ResultsExecutor executor = supplier.get();
        List<List<Solution>> inputLists = generateInputLists(columns, rows);
        List<Future<?>> futures = new ArrayList<>();
        int tasks = 10 * Runtime.getRuntime().availableProcessors();
        CountDownLatch tasksLatch = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; i++) {
            futures.add(outer.submit(() -> {
                List<MockResults> inputs = generateInput(inputLists);
                BitSet actual = new BitSet(columns);

                Results results = executor.async(inputs, null);
                tasksLatch.countDown();
                results.forEachRemainingThenClose( s -> store(actual, s));

                assertTrue(inputs.stream().allMatch(MockResults::isClosed));
            }));
        }
        tasksLatch.await();
        executor.close(); //only close after all async() calls

        for (Future<?> future : futures)
            future.get(); //throws AssertionError's
        outer.shutdown();
        assertTrue(outer.awaitTermination(1, SECONDS));
    }
}