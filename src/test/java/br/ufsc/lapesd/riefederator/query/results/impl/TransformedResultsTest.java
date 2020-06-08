package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.*;

public class TransformedResultsTest implements TestContext {

    @DataProvider
    public static Object[][] sizesData() {
        return Stream.of(0, 1, 2, 3, 10, 100, 1000, 10000).map(i -> new Object[] {i})
                      .toArray(Object[][]::new);
    }

    @Test(dataProvider = "sizesData")
    public void testPlusOne(int size) throws ExecutionException, InterruptedException, TimeoutException {
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
        TransformedResults t = new TransformedResults(inResults, singleton("x"), s -> {
            Term term = s.get(TestContext.x);
            assertNotNull(term);
            int v = Integer.parseInt(term.asLiteral().getLexicalForm());
            return MapSolution.build(TestContext.x, lit(v + 1));
        });


        List<Integer> ac = new ArrayList<>(), ex = new ArrayList<>();
        t.forEachRemainingThenClose(s ->
                ac.add(Integer.parseInt(requireNonNull(s.get(x)).asLiteral().getLexicalForm())));
        assertTrue(closed.get(0, TimeUnit.MILLISECONDS));

        for (int i = 0; i < size; i++)
            ex.add(i+1);
        assertEquals(ac, ex);
    }
}