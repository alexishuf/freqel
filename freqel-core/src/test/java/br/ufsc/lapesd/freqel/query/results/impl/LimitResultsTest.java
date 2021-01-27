package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.query.results.BufferedResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(groups = {"fast"})
public class LimitResultsTest implements TestContext {
    private static final List<BiFunction<Results, Integer, Results>> factories = asList(
            LimitResults::new,
            BufferedLimitResults::new
    );

    @DataProvider
    public static Object[][] sizeData() {
        return Stream.of(
                asList(2, 4),
                asList(1, 4),
                asList(2, 2),
                asList(4, 2),
                asList(1, 100),
                asList(10, 100),
                asList(100, 1000),
                asList(1000, 1001),
                asList(1000, 2)
        ).flatMap(list -> factories.stream().map(fac -> {
            ArrayList<Object> copy = new ArrayList<>(list);
            copy.add(0, fac);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    private static class CloseableResults extends CollectionResults {
        public boolean closed = false;

        public CloseableResults(@Nonnull Collection<? extends Solution> collection,
                                @Nonnull Collection<String> varNames) {
            super(collection, varNames);
        }

        @Override public void close() {
            super.close();
            closed = true;
        }
    }

    private @Nonnull CloseableResults createResults(int size) {
        ArraySolution.ValueFactory fac = ArraySolution.forVars(singleton("x"));
        List<Solution> list = new ArrayList<>();
        for (int i = 0; i < size; i++)
            list.add(fac.fromValues(lit(i)));
        return new CloseableResults(list, singleton("x"));
    }

    @Test(dataProvider = "sizeData")
    public void testEnforcesLimit(BiFunction<Results, Integer, Results> fac,
                                  int limit, int inputs) {
        Results limitResults = fac.apply(createResults(inputs), limit);
        CollectionResults greedy = CollectionResults.greedy(limitResults);
        assertEquals(greedy, createResults(Math.min(limit, inputs)));
    }

    @Test(dataProvider = "sizeData")
    public void testCloseAfterLimit(BiFunction<Results, Integer, Results> fac,
                                    int limit, int inputs) {
        CloseableResults inResults = createResults(inputs);
        Results limitResults = fac.apply(inResults, limit);
        assertFalse(inResults.closed);
        List<Solution> list = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            if (limitResults.hasNext())
                list.add(limitResults.next());
        }
        assertEquals(list, createResults(Math.min(limit, inputs)).getCollection());
        assertEquals(inResults.closed, limit <= inputs);
        limitResults.close(); //should not cause any error
    }

    @Test(dataProvider = "sizeData")
    public void testEnforceAndReset(BiFunction<Results, Integer, Results> fac,
                                    int limit, int inputs) {
        Results limitResults = fac.apply(createResults(inputs), limit);
        List<Solution> solutions = new ArrayList<>();

        int loops = limitResults instanceof BufferedResults ? 4 : 1;
        for (int i = 0; i < loops; i++) {
            solutions.clear();
            limitResults.forEachRemaining(solutions::add);
            assertEquals(new CollectionResults(solutions, singleton("x")),
                         createResults(Math.min(limit, inputs)));
            if (limitResults instanceof BufferedResults)
                ((BufferedResults)limitResults).reset(true);
        }
    }
}