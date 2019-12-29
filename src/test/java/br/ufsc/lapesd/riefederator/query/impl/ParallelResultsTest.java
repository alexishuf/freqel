package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Cardinality.Reliability;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class ParallelResultsTest {
    private static final String EX = "http://example.org/";

    private static class MockResults extends CollectionResults {
        private @Nonnull Cardinality cardinality;

        public MockResults(@Nonnull Collection<Solution> collection,
                           @Nonnull Set<String> varNames, @Nonnull Cardinality cardinality) {
            super(collection, varNames);
            this.cardinality = cardinality;
        }

        @Override
        public @Nonnull Cardinality getCardinality() {
            return cardinality;
        }
    }

    private static @Nonnull MockResults createMock(int count, @Nonnull String uriPrefix,
                                            @Nonnull Collection<String> varNames,
                                            @Nonnull Cardinality cardinality) {
        List<Solution> solutions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MapSolution.Builder builder = MapSolution.builder();
            for (String name : varNames)
                builder.put(name, new StdURI(uriPrefix + i));
            solutions.add(builder.build());
        }
        return new MockResults(solutions, new HashSet<>(varNames), cardinality);
    }

    @DataProvider
    public static Object[][] consumeSingleData() {
        return new Object[][] {
            new Object[]{createMock(1, EX+"1/", singleton("x"), Cardinality.exact(1))},
            new Object[]{createMock(3, EX+"1/", singleton("x"), Cardinality.exact(1))},
            new Object[]{createMock(3, EX+"1/", singleton("x"), Cardinality.guess(2))},
            new Object[]{createMock(3, EX+"1/", singleton("x"), Cardinality.lowerBound(3))},
            new Object[]{createMock(3, EX+"1/", singleton("x"), Cardinality.upperBound(4))},
        };
    }

    @Test(dataProvider = "consumeSingleData")
    public void testConsumeSingle(Results in) {
        ParallelResults parallel = new ParallelResults(singletonList(in));
        Set<Solution> all = new HashSet<>();
        parallel.forEachRemaining(all::add);

        Set<Solution> expected = new HashSet<>(((CollectionResults)in).getCollection());
        assertEquals(all, expected);
    }

    @Test(dataProvider = "consumeSingleData")
    public void testCardinalitySingle(Results in) {
        ParallelResults parallel = new ParallelResults(singletonList(in));
        assertEquals(parallel.getCardinality(), in.getCardinality());
    }

    @Test
    public void testSumExact() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.exact(3)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.exact(3))));
        assertEquals(results.getCardinality().getValue(0), 6);
        assertEquals(results.getCardinality().getReliability(), Reliability.EXACT);
    }

    @Test
    public void testSumExactAndLower() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.lowerBound(2)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.exact(3))));
        assertEquals(results.getCardinality().getValue(0), 5);
        assertEquals(results.getCardinality().getReliability(), Reliability.LOWER_BOUND);
    }

    @Test
    public void testSumExactAndUpper() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.upperBound(4)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.exact(3))));
        assertEquals(results.getCardinality().getValue(0), 7);
        assertEquals(results.getCardinality().getReliability(), Reliability.UPPER_BOUND);
    }

    @Test
    public void testSumLowerAndUpper() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.upperBound(4)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.lowerBound(2))));
        assertEquals(results.getCardinality().getValue(0), 6);
        assertEquals(results.getCardinality().getReliability(), Reliability.LOWER_BOUND);
    }

    @Test
    public void testSumLowerAndGuess() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.guess(1)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.lowerBound(2))));
        assertEquals(results.getCardinality().getValue(0), 3);
        assertEquals(results.getCardinality().getReliability(), Reliability.GUESS);
    }

    @Test
    public void testSumExactAndGuess() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.exact(3)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.guess(2))));
        assertEquals(results.getCardinality().getValue(0), 5);
        assertEquals(results.getCardinality().getReliability(), Reliability.GUESS);
    }

    @Test
    public void testSumExactAndNonEmpty() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.NON_EMPTY),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.exact(3))));
        assertEquals(results.getCardinality().getValue(0), 4);
        assertEquals(results.getCardinality().getReliability(), Reliability.LOWER_BOUND);
    }

    @Test
    public void testSumExactAndUnsupported() {
        ParallelResults results = new ParallelResults(asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.UNSUPPORTED),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.exact(3))));
        assertEquals(results.getCardinality().getValue(0), 3);
        assertEquals(results.getCardinality().getReliability(), Reliability.LOWER_BOUND);
    }

    @Test
    public void testTwoSameVar() {
        List<Results> list = asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.exact(3)),
                createMock(3, EX + "2/", singletonList("x"), Cardinality.exact(3))
        );
        ParallelResults parallelResults = new ParallelResults(list);
        assertEquals(parallelResults.getVarNames(), singleton("x"));

        Set<Solution> all = new HashSet<>(), expected = new HashSet<>();
        for (Results results : list)
            expected.addAll(((CollectionResults) results).getCollection());
        parallelResults.forEachRemaining(all::add);
        assertEquals(all, expected);
    }

    @Test
    public void testTwoDifferentVars() {
        List<Results> list = asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.exact(3)),
                createMock(3, EX + "2/", singletonList("y"), Cardinality.exact(3))
        );
        ParallelResults parallelResults = new ParallelResults(list);
        assertEquals(parallelResults.getVarNames(), Sets.newHashSet("x", "y"));

        Set<Solution> all = new HashSet<>(), expected = new HashSet<>();
        for (Results results : list)
            expected.addAll(((CollectionResults) results).getCollection());
        parallelResults.forEachRemaining(all::add);
        assertEquals(all, expected);
    }

    @Test
    public void testThreeDifferentVarsAndCardinality() {
        List<Results> list = asList(
                createMock(3, EX + "1/", singletonList("x"), Cardinality.UNSUPPORTED),
                createMock(3, EX + "2/", singletonList("y"), Cardinality.lowerBound(2)),
                createMock(3, EX + "3/", singletonList("x"), Cardinality.exact(3))
        );
        ParallelResults parallelResults = new ParallelResults(list);
        assertEquals(parallelResults.getVarNames(), Sets.newHashSet("x", "y"));

        Set<Solution> all = new HashSet<>(), expected = new HashSet<>();
        for (Results results : list)
            expected.addAll(((CollectionResults) results).getCollection());
        parallelResults.forEachRemaining(all::add);
        assertEquals(all, expected);
    }
}