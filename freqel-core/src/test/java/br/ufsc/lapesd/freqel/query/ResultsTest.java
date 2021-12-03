package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.*;
import br.ufsc.lapesd.freqel.util.NamedFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class ResultsTest implements TestContext {
    public static final @Nonnull List<NamedFunction<List<Solution>, Results>> factories;
    public static final @Nonnull List<Solution> expectedNone, expectedOne, expectedTwo,
            expectedThree, expected10;
    public static final @Nonnull StdURI Alice = new StdURI("https://example.org/Alice");
    public static final @Nonnull StdURI Bob = new StdURI("https://example.org/BOB");
    public static final @Nonnull Set<String> xSet = singleton("x");

    static {
        expectedNone = emptyList();
        expectedOne = singletonList(MapSolution.build("x", Alice));
        expectedTwo = unmodifiableList(asList(MapSolution.build("x", Alice),
                                              MapSolution.build("x", Bob)));
        expectedThree = unmodifiableList(asList(MapSolution.build("x", Alice),
                                                MapSolution.build("x", Bob),
                                                MapSolution.build("x", Charlie)));
        expected10 = unmodifiableList(IntStream.range(0, 10)
                .mapToObj(Integer::toString)
                .map(i -> MapSolution.build("x", StdLit.fromEscaped(i, V.XSD.integer)))
                .collect(Collectors.toList()));
        SPARQLFilter tautology = SPARQLFilterFactory.parseFilter("regex(str(?x), \"^http.*\") || (isNumeric(?x) && ?x >= 0)");

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
        factories.add((new NamedFunction<>("singleton ThenResults",
                coll -> new ThenResults(xSet,
                                        singleton(() -> new CollectionResults(coll, xSet))))));
        factories.add(new NamedFunction<>("Flux-based PublisherResults",
                coll -> new PublisherResults(Flux.fromIterable(coll), xSet)));
        factories.add(new NamedFunction<>("1-1 Flux-based PublisherResults",
                coll -> new PublisherResults(Flux.fromIterable(coll), xSet, 1, 1)));
        factories.add(new NamedFunction<>("5-2 Flux-based PublisherResults",
                coll -> new PublisherResults(Flux.fromIterable(coll), xSet, 5, 2)));
        factories.add(new NamedFunction<>("10-1 Flux-based PublisherResults",
                coll -> new PublisherResults(Flux.fromIterable(coll), xSet, 10, 1)));
        factories.add(new NamedFunction<>("10-5 Flux-based PublisherResults",
                coll -> new PublisherResults(Flux.fromIterable(coll), xSet, 10, 5)));
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
        collectionTest(fac, expectedTwo);
    }

    @Test(dataProvider = "factoriesData")
    public void testThree(Function<Collection<Solution>, Results> fac) {
        collectionTest(fac, expectedThree);
    }

    @Test(dataProvider = "factoriesData")
    public void test10(Function<Collection<Solution>, Results> fac) {
        collectionTest(fac, expected10);
    }

    @Test(dataProvider = "factoriesData")
    public void testExhaust(Function<Collection<Solution>, Results> fac) {
        try (Results it = fac.apply(expectedTwo)) {
            while (it.hasNext())
                it.next();
            assertEquals(it.getReadyCount(), 0);
        }
    }
}