package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class FlatMapResultsTest implements TestContext {

    @DataProvider
    public static Object[][] sizesData() {
        return Stream.of(1, 2, 3, 10, 100, 1000)
                .flatMap(s -> Stream.of(0, 1, 2, 3).map(ss -> new Object[] {s, ss}))
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "sizesData")
    public void testMultiples(int size, int subSize) throws InterruptedException, ExecutionException, TimeoutException {
        List<Solution> in = new ArrayList<>();
        for (int i = 0; i < size; i++)
            in.add(MapSolution.build(x, lit(i)));

        CompletableFuture<Boolean> closed = new CompletableFuture<>();
        CollectionResults inResults = new CollectionResults(in, singleton("x")) {
            @Override
            public void close() {
                super.close();
                closed.complete(true);
            }
        };

        FlatMapResults results = new FlatMapResults(inResults, singleton("x"), s -> {
            Term term = s.get(x);
            assertNotNull(term);
            int v = parseInt(term.asLiteral().getLexicalForm());
            List<Solution> list = new ArrayList<>();
            for (int i = 0; i < subSize; i++) {
                list.add(MapSolution.build(x, lit((v + i) * -1)));
            }
            return new CollectionResults(list, singleton("x"));
        });

        List<Integer> ac = new ArrayList<>(), ex = new ArrayList<>();
        results.forEachRemainingThenClose(s ->
            ac.add(parseInt(Objects.requireNonNull(s.get(x)).asLiteral().getLexicalForm())));
        assertTrue(closed.get(0, TimeUnit.MILLISECONDS));

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < subSize; j++)
                ex.add((i + j) * -1);
        }
        assertEquals(ac, ex);
    }

}