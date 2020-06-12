package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.IteratorResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class ResultsTest implements TestContext {
    public static final @Nonnull List<NamedFunction<List<Solution>, Results>> factories;
    public static final @Nonnull List<Solution> expectedNone, expectedOne, expectedTWo;
    public static final @Nonnull StdURI Alice = new StdURI("https://example.org/Alice");
    public static final @Nonnull StdURI Bob = new StdURI("https://example.org/BOB");
    public static final @Nonnull Set<String> xSet = unmodifiableSet(singleton("x"));

    static {
        expectedNone = unmodifiableList(emptyList());
        expectedOne = unmodifiableList(singletonList(MapSolution.build("x", Alice)));
        expectedTWo = unmodifiableList(asList(MapSolution.build("x", Alice),
                                              MapSolution.build("x", Bob)));
        SPARQLFilter tautology = SPARQLFilter.build("regex(str(?x), \"^http.*\")");

        factories = new ArrayList<>();
        factories.add(new NamedFunction<>("CollectionSolutionIterator",
                coll -> new CollectionResults(coll, xSet)));
        factories.add(new NamedFunction<>("IteratorResults",
                coll -> new IteratorResults(coll.iterator(), xSet)));
        factories.add(new NamedFunction<>("Tautology SPARQLFilterResults",
                coll -> new SPARQLFilterResults(new CollectionResults(coll, xSet),
                                                singleton(tautology))));
    }

    @DataProvider
    public static @Nonnull Object[][] factoriesData() {
        return factories.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    protected void collectionTest(Function<Collection<Solution>, Results> factory,
                                  @Nonnull Collection<Solution> expected) {
        List<Solution> actual = new ArrayList<>();
        try (Results it = factory.apply(expected)) {
            HashSet<String> startVars = new HashSet<>(it.getVarNames());

            while (it.hasNext())
                actual.add(it.next());
            assertEquals(actual, new ArrayList<>(expected));
            assertEquals(it.getVarNames(), startVars);
        }
    }

    @Test(dataProvider = "factoriesData")
    public void testVarNames(Function<Collection<Solution>, Results> fac) {
        try (Results it = fac.apply(expectedOne)) {
            assertEquals(it.getVarNames(), xSet);
        }
        try (Results it = fac.apply(expectedNone)) {
            assertEquals(it.getVarNames(), xSet);
        }
    }

    @Test(dataProvider = "factoriesData")
    public void testEmpty(Function<Collection<Solution>, Results> fac) {
        collectionTest(fac, expectedNone);
    }

    @Test(dataProvider = "factoriesData")
    public void testOne(Function<Collection<Solution>, Results> fac) {
        collectionTest(fac, expectedOne);
    }

    @Test(dataProvider = "factoriesData")
    public void testTwo(Function<Collection<Solution>, Results> fac) {
        collectionTest(fac, expectedTWo);
    }

    @Test(dataProvider = "factoriesData")
    public void testExhaust(Function<Collection<Solution>, Results> fac) {
        try (Results it = fac.apply(expectedTWo)) {
            while (it.hasNext()) {
                assertTrue(it.getReadyCount() > 0);
                it.next();
            }
            assertEquals(it.getReadyCount(), 0);
        }
    }
}