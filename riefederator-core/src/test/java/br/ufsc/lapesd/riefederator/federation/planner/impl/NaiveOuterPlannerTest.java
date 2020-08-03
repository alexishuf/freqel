package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class NaiveOuterPlannerTest implements TestContext {

    @DataProvider
    public static Object[][] cartesianComponentsData() {
        return Stream.of(
                asList(singleton(new Triple(Alice, p1, x)),
                        singleton(singleton(new Triple(Alice, p1, x)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(y, p2, Bob)),
                        asList(singleton(new Triple(Alice, p1, x)),
                                singleton(new Triple(y, p2, Bob)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Alice)),
                        asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y)),
                                singleton(new Triple(z, p3, Alice)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob)),
                        asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y)),
                                singleton(new Triple(z, p3, Bob)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob), new Triple(z, p4, Bob)),
                        asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y)),
                                asList(new Triple(z, p3, Bob), new Triple(z, p4, Bob)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob), new Triple(z, p4, y)),
                        singleton(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                                new Triple(z, p3, Bob), new Triple(z, p4, y)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob), new Triple(z, p4, x)),
                        singleton(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                                new Triple(z, p3, Bob), new Triple(z, p4, x)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(Alice, p2, y)),
                        asList(singleton(new Triple(Alice, p1, x)),
                                singleton(new Triple(Alice, p2, y))))
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "cartesianComponentsData")
    public void testCartesianComponents(Collection<Triple> triples,
                                        Collection<Collection<Triple>> expected) {
        NaiveOuterPlanner planner = new NaiveOuterPlanner();
        List<IndexedSet<Triple>> list = planner.getCartesianComponents(IndexedSet.from(triples));
        assertEquals(new HashSet<>(list), expected.stream().map(IndexedSet::from).collect(toSet()));

        assertEquals(MutableCQuery.from(triples).attr().isJoinConnected(), expected.size()==1);
    }
}