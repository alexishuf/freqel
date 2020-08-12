package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.FreeQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.planner.impl.NaiveOuterPlanner;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class OuterPlannerTest implements TestContext {

    public static @Nonnull List<Class<? extends OuterPlanner>> classes = singletonList(
            NaiveOuterPlanner.class
    );

    @DataProvider
    public static @Nonnull Object[][] planData() throws IOException, SPARQLParseException {
        List<List<Object>> stubs = new ArrayList<>(asList(
                asList(createQuery(Alice, knows, x), singleton(createQuery(Alice, knows, x))),
                asList(createQuery(x, knows, y), singleton(createQuery(x, knows, y))),
                asList(createQuery(Alice, knows, Bob, Ask.REQUIRED),
                        singleton(createQuery(Alice, knows, Bob, Ask.REQUIRED))),
                asList(createQuery(Alice, knows, x, x, knows, y),
                       singleton(createQuery(Alice, knows, x, x, knows, y))),
                asList(createQuery(Alice, knows, x, x, knows, y, Ask.REQUIRED),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Ask.REQUIRED))),
                asList(createQuery(Alice, knows, x, x, knows, y, Ask.ADVISED),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Ask.ADVISED))),
                asList(createQuery(Alice, knows, x, x, knows, y, Projection.required("x")),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Projection.required("x")))),
                asList(createQuery(Alice, knows, x, x, knows, y, Projection.advised("x")),
                       singleton(createQuery(Alice, knows, x, x, knows, y, Projection.advised("x")))),
                asList(createQuery(Alice, name, x, Alice, age, y),
                       asList(createQuery(Alice, name, x),
                              createQuery(Alice, age, y))),
                asList(createQuery(Alice, name, x, Alice, age, y, Ask.ADVISED),
                       asList(createQuery(Alice, name, x),
                              createQuery(Alice, age, y))),
                asList(createQuery(Alice, name, x, Alice, age, y, Projection.required("x")),
                       asList(createQuery(Alice, name, x),
                              createQuery(Alice, age, y))),
                asList(createQuery(Alice, knows, x, x, name, z, Alice, age, y, Projection.required("z")),
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
                           SPARQLFilter.build("str(?u) = str(?v)") //only assignable to the root
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
            if (op instanceof FreeQueryOp) {
                MutableCQuery query = ((FreeQueryOp) op).getQuery();
                stubs.add(asList(query, singleton(query)));
            }
        }

        List<List<Object>> rows = new ArrayList<>();
        for (Class<? extends OuterPlanner> aClass : classes) {
            for (List<Object> stub : stubs) {
                ArrayList<Object> row = new ArrayList<>(stub);
                row.add(0, new NamedSupplier<>(aClass));
                rows.add(row);
            }
        }
        return rows.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlan(Supplier<OuterPlanner> supplier,
                         Object query, Collection<CQuery> expectedComponents) {
        OuterPlanner planner = supplier.get();
        Op asTree = query instanceof Op ? (Op)query : new FreeQueryOp((CQuery)query);
        Op plan = planner.plan(asTree);
        Set<Op> leaves = TreeUtils.streamPreOrder(plan)
                .filter(FreeQueryOp.class::isInstance).collect(toSet());
        assertTrue(leaves.stream().allMatch(FreeQueryOp.class::isInstance));
        assertTrue(leaves.stream().noneMatch(QueryOp.class::isInstance));
        PlannerTest.assertPlanAnswers(plan, asTree);

        Set<CQuery> actual = leaves.stream().map(n -> ((FreeQueryOp) n).getQuery()).collect(toSet());
        
        HashSet<CQuery> expected = new HashSet<>(expectedComponents);

        assertTrue(expected.stream().allMatch(e -> actual.stream()
                .anyMatch(a -> a.attr().getSet().equals(e.attr().getSet())
                            && a.getModifiers().equals(e.getModifiers()))));
        assertTrue(actual.stream().allMatch(a -> expected.stream()
                .anyMatch(e -> e.attr().getSet().equals(a.attr().getSet())
                        && e.getModifiers().equals(a.getModifiers()))));

        if (asTree instanceof FreeQueryOp) {
            CQuery cQuery = ((FreeQueryOp) asTree).getQuery();
            assertEquals(cQuery.attr().isJoinConnected(), expectedComponents.size()==1);
        }
    }
}