package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class ConjunctionReplaceStepTest implements TestContext {

    @DataProvider
    public static Object[][] testData() {
        QueryOp q1 = new QueryOp(createQuery(Alice, knows, x));
        return Stream.of(
                asList(q1, emptySet(), null),
                asList(UnionOp.builder().add(q1).add(new QueryOp(createQuery(Bob, knows, x))).build(),
                       emptySet(), null),
                asList(UnionOp.builder().add(q1).add(new QueryOp(createQuery(Bob, knows, x))).build(),
                       singleton(q1), null),
                //base case with a single join
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(
                                        Alice, knows, x,
                                        x, age, u, SPARQLFilter.build("?u < 23"))))
                                .add(new QueryOp(createQuery(x, knows, Bob)))
                                .add(Projection.of("u"))
                                .build(),
                       emptySet(),
                       JoinOp.builder(
                               new QueryOp(createQuery(
                                       Alice, knows, x,
                                       x, age, u, SPARQLFilter.build("?u < 23"))),
                               new QueryOp(createQuery(x, knows, Bob))
                       ).add(Projection.of("u")).build()),
                //base case with a single product
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, knows, x)))
                                .add(new QueryOp(createQuery(
                                        Alice, age, u, SPARQLFilter.build("?u < 23"))))
                                .add(Projection.of("u"))
                                .build(),
                       emptySet(),
                       CartesianOp.builder()
                               .add(new QueryOp(createQuery(Alice, knows, x)))
                               .add(new QueryOp(createQuery(
                                       Alice, age, u, SPARQLFilter.build("?u < 23"))))
                               .add(Projection.of("u"))
                               .build()),
                //apply base case with join in child
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, knows, x)))
                                .add(ConjunctionOp.builder()
                                        .add(new QueryOp(createQuery(Bob, knows, y)))
                                        .add(new QueryOp(createQuery(
                                                y, age, u, SPARQLFilter.build("?u < 23"))))
                                        .build())
                                .add(Projection.of("y", "u"))
                                .build(),
                       emptySet(),
                       CartesianOp.builder()
                               .add(new QueryOp(createQuery(Alice, knows, x)))
                               .add(JoinOp.create(
                                       new QueryOp(createQuery(Bob, knows, y)),
                                       new QueryOp(createQuery
                                               (y, age, u, SPARQLFilter.build("?u < 23")))))
                               .add(Projection.of("y", "u"))
                               .build()),
                // do not project-out vars used in filters
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(x, age, u)))
                                .add(new QueryOp(createQuery(x, age, v, x, name, w)))
                                .add(SPARQLFilter.build("?u < ?v"))
                                .add(Projection.of("x"))
                                .build(),
                       emptySet(),
                       JoinOp.builder(new QueryOp(createQuery(x, age, u)),
                                      new QueryOp(createQuery(x, age, v, x, name, w)))
                               .add(SPARQLFilter.build("?u < ?v"))
                               .add(Projection.of("x"))
                               .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nonnull Set<RefEquals<Op>> locked, @Nullable Op expected) {
        if (expected == null)
            expected = in;
        boolean equalsIn = expected.equals(in);
        JoinOrderPlanner joPlanner =
                SingletonSourceFederation.getInjector().getInstance(JoinOrderPlanner.class);
        ConjunctionReplaceStep step = new ConjunctionReplaceStep(joPlanner);
        Op actual = step.plan(in, locked);
        assertEquals(actual, expected);
        if (equalsIn)
            assertSame(actual, in);
    }

}