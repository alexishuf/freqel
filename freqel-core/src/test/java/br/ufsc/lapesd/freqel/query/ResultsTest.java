package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.*;
import br.ufsc.lapesd.freqel.util.NamedFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class ResultsTest implements TestContext {
    public static final @Nonnull List<NamedFunction<List<Solution>, Results>> factories;
    public static final @Nonnull List<Solution> expectedNone, expectedOne, expectedTWo;
    public static final @Nonnull StdURI Alice = new StdURI("https://example.org/Alice");
    public static final @Nonnull StdURI Bob = new StdURI("https://example.org/BOB");
    public static final @Nonnull Set<String> xSet = singleton("x");

    static {
        expectedNone = emptyList();
        expectedOne = singletonList(MapSolution.build("x", Alice));
        expectedTWo = unmodifiableList(asList(MapSolution.build("x", Alice),
                                              MapSolution.build("x", Bob)));
        SPARQLFilter tautology = SPARQLFilterFactory.parseFilter("regex(str(?x), \"^http.*\")");

        factories = new ArrayList<>();
        factories.add(new NamedFunction<>("CollectionResults",
                coll -> new CollectionResults(coll, xSet)));
        factories.add(new NamedFunction<>("IteratorResults",
                coll -> new IteratorResults(coll.iterator(), xSet)));
        factories.add(new NamedFunction<>("Tautology SPARQLFilterResults",
                coll -> new SPARQLFilterResults(new CollectionResults(coll, xSet),
                                                singleton(tautology))));
        factories.add(new NamedFunction<>("SequentialResults",
                coll -> new SequentialResults(singleton(new CollectionResults(coll, xSet)), xSet)));
        factories.add(new NamedFunction<>("identity TransformedResults",
                coll -> new TransformedResults(new CollectionResults(coll, xSet),
                                               xSet, Function.identity())));
        factories.add(new NamedFunction<>("singleton FlatMapResults",
                coll -> new FlatMapResults(new CollectionResults(coll, xSet), xSet,
                                           s -> new CollectionResults(singleton(s), xSet))));
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
            while (it.hasNext())
                it.next();
            assertEquals(it.getReadyCount(), 0);
        }
    }
}