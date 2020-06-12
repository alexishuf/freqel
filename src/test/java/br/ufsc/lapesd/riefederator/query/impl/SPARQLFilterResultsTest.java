package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.IteratorResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class SPARQLFilterResultsTest implements TestContext {

    @DataProvider
    public static Object[][] numbersData() {

        return Stream.of(
                asList(asList(1, 2, 3, 4),
                       singletonList(SPARQLFilter.build("?x > 1")),
                       asList(2, 3, 4)),
                asList(asList(1, 2, 3, 4),
                       singletonList(SPARQLFilter.build("?x < 4")),
                       asList(1, 2, 3)),
                asList(asList(1, 2, 3, 4, 5, 6),
                       asList(SPARQLFilter.build("?x > 2"),
                              SPARQLFilter.build("?x < 5")),
                       asList(3, 4)),
                asList(asList(1, 2, 3, 4, 5, 6),
                       asList(SPARQLFilter.build("?x > 2"),
                              SPARQLFilter.build("?x < 6"),
                              SPARQLFilter.build("?x != 4")),
                       asList(3, 5))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    private void doFilterNumbersTest(Results inner, List<SPARQLFilter> filters,
                                     List<Integer> expected) {
        SPARQLFilterResults results = new SPARQLFilterResults(inner, filters);
        List<Integer> actual = new ArrayList<>();
        results.forEachRemainingThenClose(s -> actual.add(parseInt(
                requireNonNull(s.get(x)).asLiteral().getLexicalForm())));

        assertEquals(actual, expected);
    }

    @Test(dataProvider = "numbersData")
    public void testFilterNumbersOnCollection(List<Integer> numbers, List<SPARQLFilter> filters,
                                              List<Integer> expected) {
        List<Solution> solutions = numbers.stream()
                .map(i -> MapSolution.build("x", lit(i))).collect(toList());
        doFilterNumbersTest(new CollectionResults(solutions, singleton("x")), filters, expected);
    }

    @Test(dataProvider = "numbersData")
    public void testFilterNumbersOnIterator(List<Integer> numbers, List<SPARQLFilter> filters,
                                              List<Integer> expected) {
        List<Solution> solutions = numbers.stream()
                .map(i -> MapSolution.build("x", lit(i))).collect(toList());
        doFilterNumbersTest(new IteratorResults(solutions.iterator(), singleton("x")),
                            filters, expected);
    }

}