package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.impl.CollectionSolutionIterator;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.EXACT;
import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.UPPER_BOUND;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class SolutionIteratorTest {
    public static final @Nonnull List<NamedFunction<List<Solution>, SolutionIterator>> factories;
    public static final @Nonnull List<Solution> expectedNone, expectedOne, expectedTWo;
    public static final @Nonnull StdURI ALICE = new StdURI("https://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("https://example.org/BOB");
    public static final @Nonnull Set<String> xSet = unmodifiableSet(singleton("x"));

    static {
        expectedNone = unmodifiableList(emptyList());
        expectedOne = unmodifiableList(singletonList(MapSolution.build("x", ALICE)));
        expectedTWo = unmodifiableList(asList(MapSolution.build("x", ALICE),
                                              MapSolution.build("x", BOB)));

        factories = new ArrayList<>();
        factories.add(new NamedFunction<>("CollectionSolutionIterator",
                coll -> new CollectionSolutionIterator(coll, xSet)));
    }

    @DataProvider
    public static @Nonnull Object[][] factoriesData() {
        return factories.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    protected void collectionTest(Function<Collection<Solution>, SolutionIterator> factory,
                                  @Nonnull Collection<Solution> expected) {
        List<Solution> actual = new ArrayList<>();
        SolutionIterator it = factory.apply(expected);
        HashSet<String> startVars = new HashSet<>(it.getVarNames());

        assertNotNull(it.getCardinality());
        int size = expected.size();
        if (it.getCardinality().getReliability().ordinal() >= UPPER_BOUND.ordinal())
            assertEquals(it.getCardinality().getValue(-1), size);

        while (it.hasNext()) {
            actual.add(it.next());
            --size;
            if (it.getCardinality().getReliability().ordinal() >= UPPER_BOUND.ordinal())
                assertEquals(it.getCardinality().getValue(-1), size);
        }
        assertEquals(actual, new ArrayList<>(expected));
        assertEquals(it.getVarNames(), startVars);
    }

    @Test(dataProvider = "factoriesData")
    public void testVarNames(Function<Collection<Solution>, SolutionIterator> fac) {
        SolutionIterator it = fac.apply(expectedOne);
        assertEquals(it.getVarNames(), xSet);

        it = fac.apply(expectedNone);
        assertEquals(it.getVarNames(), xSet);
    }

    @Test(dataProvider = "factoriesData")
    public void testEmpty(Function<Collection<Solution>, SolutionIterator> fac) {
        collectionTest(fac, expectedNone);
    }

    @Test(dataProvider = "factoriesData")
    public void testOne(Function<Collection<Solution>, SolutionIterator> fac) {
        collectionTest(fac, expectedOne);
    }

    @Test(dataProvider = "factoriesData")
    public void testTwo(Function<Collection<Solution>, SolutionIterator> fac) {
        collectionTest(fac, expectedTWo);
    }

    @Test(dataProvider = "factoriesData")
    public void testExhaust(Function<Collection<Solution>, SolutionIterator> fac) {
        SolutionIterator it = fac.apply(expectedTWo);
        int size = expectedTWo.size();
        if (it.getCardinality().getReliability() == EXACT)
            assertEquals(it.getReadyCount(), size);
        if (it.getCardinality().getReliability().ordinal() >= UPPER_BOUND.ordinal())
            assertEquals(it.getCardinality().getValue(-1), size);
        while (it.hasNext()) {
            it.next();
            --size;
            if (it.getCardinality().getReliability().ordinal() >= UPPER_BOUND.ordinal())
                assertEquals(it.getCardinality().getValue(-1), size);
            if (it.getCardinality().getReliability() == EXACT)
                assertEquals(it.getReadyCount(), size);
        }
        assertEquals(it.getReadyCount(), 0);
        assertEquals(it.getCardinality(), new Cardinality(EXACT, 0));
    }
}