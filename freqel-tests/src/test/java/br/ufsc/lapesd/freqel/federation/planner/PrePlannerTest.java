package br.ufsc.lapesd.freqel.federation.planner;

import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.PlanAssert;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.modifiers.Ask;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.util.NamedSupplier;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.inject.Provider;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class PrePlannerTest implements TestContext {

    public static @Nonnull List<Provider<? extends PrePlanner>> providers = singletonList(
            new NamedSupplier<>("default",
                    () -> DaggerTestComponent.builder().build().prePlanner())
    );

    @DataProvider
    public static @Nonnull Object[][] planData() throws IOException, SPARQLParseException {
        List<List<Object>> stubs = new ArrayList<>(asList(
                asList(createQuery(Alice, knows, x), singleton(createQuery(Alice, knows, x))),
                asList(createQuery(x, knows, y), singleton(createQuery(x, knows, y))),
                asList(createQuery(Alice, knows, Bob, Ask.INSTANCE),
                        singleton(createQuery(Alice, knows, Bob, Ask.INSTANCE))),
                asList(createQuery(Alice, knows, x, x, knows, y),
                       singleton(createQuery(Alice, knows, x, x, knows, y))),
                asList(createQuery(Alice, knows, x, x, knows, y, Ask.INSTANCE),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Ask.INSTANCE))),
                asList(createQuery(Alice, knows, x, x, knows, y, Ask.INSTANCE),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Ask.INSTANCE))),
                asList(createQuery(Alice, knows, x, x, knows, y, Projection.of("x")),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Projection.of("x")))),
                asList(createQuery(Alice, knows, x, x, knows, y, Projection.of("x")),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Projection.of("x")))),
                asList(createQuery(Alice, name, x, Alice, age, y),
                       asList(createQuery(Alice, name, x),
                              createQuery(Alice, age, y))),
                asList(createQuery(Alice, name, x, Alice, age, y, Ask.INSTANCE),
                       asList(createQuery(Alice, name, x),
                              createQuery(Alice, age, y))),
                asList(createQuery(Alice, name, x, Alice, age, y, Projection.of("x")),
                       asList(createQuery(Alice, name, x),
                              createQuery(Alice, age, y))),
                asList(createQuery(Alice, knows, x, x, name, z, Alice, age, y, Projection.of("z")),
                       asList(createQuery(Alice, knows, x, x, name, z),
                              createQuery(Alice, age, y))),
                asList(createQuery( /* simplified from LargeRDFBench's B6 */
                           s, p1, Alice,
                           s, p2, x,
                           x, type, Class1,
                           x, p3, u,

                           y, p4, z,
                           z, p5, w,
                           w, p6, v,
                           JenaSPARQLFilter.build("str(?u) = str(?v)") //only assignable to the root
                       ),
                       asList(createQuery(
                                      s, p1, Alice,
                                      s, p2, x,
                                      x, type, Class1,
                                      x, p3, u
                              ),
                              createQuery(
                                      y, p4, z,
                                      z, p5, w,
                                      w, p6, v
                              )
                       ))
                ));
        List<String> queryNames = new ArrayList<>(LargeRDFBenchSelfTest.QUERY_FILENAMES);
        queryNames.remove("S2"); // has cartesian product
        queryNames.remove("B5"); // has cartesian product
        queryNames.remove("B6"); // has cartesian product
        for (String queryName : queryNames) {
            Op op = LargeRDFBenchSelfTest.loadQuery(queryName);
            if (op instanceof QueryOp) {
                MutableCQuery query = ((QueryOp) op).getQuery();
                stubs.add(asList(query, singleton(query)));
            }
        }

        List<List<Object>> rows = new ArrayList<>();
        for (Provider<? extends PrePlanner> supplier : providers) {
            for (List<Object> stub : stubs) {
                ArrayList<Object> row = new ArrayList<>(stub);
                row.add(0, supplier);
                rows.add(row);
            }
        }
        return rows.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlan(Supplier<PrePlanner> supplier,
                         Object query, Collection<CQuery> expectedComponents) {
        PrePlanner planner = supplier.get();
        Op asTree = query instanceof Op ? (Op)query : new QueryOp((CQuery)query);
        Op plan = planner.plan(asTree);
        Set<Op> leaves = TreeUtils.streamPreOrder(plan)
                .filter(QueryOp.class::isInstance).collect(toSet());
        assertTrue(leaves.stream().allMatch(QueryOp.class::isInstance));
        assertTrue(leaves.stream().noneMatch(EndpointQueryOp.class::isInstance));
        PlanAssert.assertPlanAnswers(plan, asTree);

        Set<CQuery> actual = leaves.stream().map(n -> ((QueryOp) n).getQuery()).collect(toSet());
        
        HashSet<CQuery> expected = new HashSet<>(expectedComponents);

        assertTrue(expected.stream().allMatch(e -> actual.stream()
                .anyMatch(a -> a.attr().getSet().equals(e.attr().getSet())
                            && a.getModifiers().equals(e.getModifiers()))));
        assertTrue(actual.stream().allMatch(a -> expected.stream()
                .anyMatch(e -> e.attr().getSet().equals(a.attr().getSet())
                        && e.getModifiers().equals(a.getModifiers()))));

        if (asTree instanceof QueryOp) {
            CQuery cQuery = ((QueryOp) asTree).getQuery();
            assertEquals(cQuery.attr().isJoinConnected(), expectedComponents.size()==1);
        }
    }

    @DataProvider
    public static Object[][] exactTreeData() {
        return Stream.of(
                // CQuery in, CQuery out
                asList(new QueryOp(createQuery(x, knows, Alice)),
                        new QueryOp(createQuery(x, knows, Alice))),
                // CQuery+filters in, CQuery+filters out
                asList(new QueryOp(createQuery(x, knows, Alice,
                        x, age, u,
                        JenaSPARQLFilter.build("?u > 23"))),
                        new QueryOp(createQuery(x, knows, Alice,
                                x, age,   u,
                                JenaSPARQLFilter.build("?u > 23")))),
                // join-disconnected query. add cartesian and redistribute filters
                asList(new QueryOp(createQuery(x,     knows, Alice,
                        x,     age,   u, JenaSPARQLFilter.build("?u < ?v"),
                        Alice, knows, y,
                        y,     age, v, JenaSPARQLFilter.build("?v > 23"))),
                        CartesianOp.builder()
                                .add(new QueryOp(createQuery(x, knows, Alice,
                                                             x, age, u)))
                                .add(new QueryOp(createQuery(Alice, knows, y,
                                                             y,     age, v,
                                                             JenaSPARQLFilter.build("?v > 23"))))
                                .add(JenaSPARQLFilter.build("?u < ?v")).build()),
                // union in, union out, preserving filters
                asList(UnionOp.builder()
                                .add(new QueryOp(createQuery(
                                        x, knows, Bob,
                                        x, age, u, JenaSPARQLFilter.build("?u > 23"))))
                                .add(new QueryOp(createQuery(
                                        x, knows, Alice,
                                        x, age, v, JenaSPARQLFilter.build("?v < 23")))).build(),
                        UnionOp.builder()
                                .add(new QueryOp(createQuery(
                                        x, knows, Bob,
                                        x, age, u, JenaSPARQLFilter.build("?u > 23"))))
                                .add(new QueryOp(createQuery(
                                        x, knows, Alice,
                                        x, age, v, JenaSPARQLFilter.build("?v < 23")))).build()),
                // push query op into union
                asList(ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(Alice, knows, x)))
                                        .add(new QueryOp(createQuery(Bob, knows, x)))
                                        .build())
                                .add(new QueryOp(
                                        createQuery(x, age, u, JenaSPARQLFilter.build("?u < 23"))))
                                .build(),
                        UnionOp.builder()
                                .add(new QueryOp(createQuery(
                                        Alice, knows, x,
                                        x,     age,   u, JenaSPARQLFilter.build("?u < 23")
                                )))
                                .add(new QueryOp(createQuery(
                                        Bob, knows, x,
                                        x,   age,   u, JenaSPARQLFilter.build("?u < 23")
                                )))
                                .build()),
                // push query op into product
                asList(ConjunctionOp.builder()
                                .add(CartesianOp.builder()
                                        .add(new QueryOp(createQuery(
                                                x, knows, Alice,
                                                x, age,   u, JenaSPARQLFilter.build("?u < 23")
                                        )))
                                        .add(new QueryOp(createQuery(
                                                y, knows, Bob,
                                                y, age,   v, JenaSPARQLFilter.build("?v > 23")
                                        )))
                                        .build())
                                .add(new QueryOp(createQuery(
                                        x, knows, z,
                                        z, age, w, JenaSPARQLFilter.build("?w > 5")
                                )))
                                .build(),
                        CartesianOp.builder()
                                .add(new QueryOp(createQuery(
                                        x, knows, Alice,
                                        x, age, u, JenaSPARQLFilter.build("?u < 23"),
                                        x, knows, z,
                                        z, age, w, JenaSPARQLFilter.build("?w > 5")
                                )))
                                .add(new QueryOp(createQuery(
                                        y, knows, Bob,
                                        y, age, v, JenaSPARQLFilter.build("?v > 23")
                                )))
                                .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "exactTreeData")
    public void testExactTree(@Nonnull Op in, @Nonnull Op expected) {
        PrePlanner planner = DaggerTestComponent.builder().build().prePlanner();
        Op actual = planner.plan(in);
        assertEquals(actual, expected);
    }
}