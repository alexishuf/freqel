package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.FreeQueryOp;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
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

    @DataProvider
    public static Object[][] planData() {
        return Stream.of(
                // CQuery in, CQuery out
                asList(new FreeQueryOp(createQuery(x, knows, Alice)),
                       new FreeQueryOp(createQuery(x, knows, Alice))),
                // CQuery+filters in, CQuery+filters out
                asList(new FreeQueryOp(createQuery(x, knows, Alice,
                                                   x, age, u,
                                                   SPARQLFilter.build("?u > 23"))),
                       new FreeQueryOp(createQuery(x, knows, Alice,
                                                   x, age,   u,
                                                   SPARQLFilter.build("?u > 23")))),
                // join-disconnected query. add cartesian and redistribute filters
                asList(new FreeQueryOp(createQuery(x,     knows, Alice,
                                                   x,     age,   u, SPARQLFilter.build("?u < ?v"),
                                                   Alice, knows, y,
                                                   y,     age, v, SPARQLFilter.build("?v > 23"))),
                       CartesianOp.builder()
                               .add(new FreeQueryOp(createQuery(x, knows, Alice,
                                                                x, age, u)))
                               .add(new FreeQueryOp(createQuery(Alice, knows, y,
                                                                y,     age, v,
                                                                SPARQLFilter.build("?v > 23"))))
                               .add(SPARQLFilter.build("?u < ?v")).build()),
                // union in, union out, preserving filters
                asList(UnionOp.builder()
                                .add(new FreeQueryOp(createQuery(
                                        x, knows, Bob,
                                        x, age, u, SPARQLFilter.build("?u > 23"))))
                                .add(new FreeQueryOp(createQuery(
                                        x, knows, Alice,
                                        x, age, v, SPARQLFilter.build("?v < 23")))).build(),
                        UnionOp.builder()
                                .add(new FreeQueryOp(createQuery(
                                        x, knows, Bob,
                                        x, age, u, SPARQLFilter.build("?u > 23"))))
                                .add(new FreeQueryOp(createQuery(
                                        x, knows, Alice,
                                        x, age, v, SPARQLFilter.build("?v < 23")))).build()),
                // push query op into union
                asList(ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new FreeQueryOp(createQuery(Alice, knows, x)))
                                        .add(new FreeQueryOp(createQuery(Bob, knows, x)))
                                        .build())
                                .add(new FreeQueryOp(
                                        createQuery(x, age, u, SPARQLFilter.build("?u < 23"))))
                                .build(),
                        UnionOp.builder()
                                .add(new FreeQueryOp(createQuery(
                                        Alice, knows, x,
                                        x,     age,   u, SPARQLFilter.build("?u < 23")
                                )))
                                .add(new FreeQueryOp(createQuery(
                                        Bob, knows, x,
                                        x,   age,   u, SPARQLFilter.build("?u < 23")
                                )))
                                .build()),
                // push query op into product
                asList(ConjunctionOp.builder()
                               .add(CartesianOp.builder()
                                       .add(new FreeQueryOp(createQuery(
                                               x, knows, Alice,
                                               x, age,   u, SPARQLFilter.build("?u < 23")
                                       )))
                                       .add(new FreeQueryOp(createQuery(
                                               y, knows, Bob,
                                               y, age,   v, SPARQLFilter.build("?v > 23")
                                       )))
                                       .build())
                               .add(new FreeQueryOp(createQuery(
                                       x, knows, z,
                                       z, age, w, SPARQLFilter.build("?w > 5")
                               )))
                               .build(),
                       CartesianOp.builder()
                               .add(new FreeQueryOp(createQuery(
                                       x, knows, Alice,
                                       x, age, u, SPARQLFilter.build("?u < 23"),
                                       x, knows, z,
                                       z, age, w, SPARQLFilter.build("?w > 5")
                               )))
                               .add(new FreeQueryOp(createQuery(
                                       y, knows, Bob,
                                       y, age, v, SPARQLFilter.build("?v > 23")
                               )))
                               .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlan(@Nonnull Op in, @Nonnull Op expected) {
        NaiveOuterPlanner planner = new NaiveOuterPlanner();
        Op actual = planner.plan(in);
        assertEquals(actual, expected);
    }
}