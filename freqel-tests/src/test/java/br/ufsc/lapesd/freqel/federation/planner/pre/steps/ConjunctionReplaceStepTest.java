package br.ufsc.lapesd.freqel.federation.planner.pre.steps;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.util.ref.EmptyRefSet;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import br.ufsc.lapesd.freqel.util.ref.RefSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class ConjunctionReplaceStepTest implements TestContext {

    @DataProvider
    public static Object[][] testData() {
        QueryOp q1 = new QueryOp(createQuery(Alice, knows, x));
        return Stream.of(
                asList(q1, EmptyRefSet.emptySet(), null),
                asList(UnionOp.builder().add(q1).add(new QueryOp(createQuery(Bob, knows, x))).build(),
                       EmptyRefSet.emptySet(), null),
                asList(UnionOp.builder().add(q1).add(new QueryOp(createQuery(Bob, knows, x))).build(),
                       IdentityHashSet.of(q1), null),
                //base case with a single join
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(
                                        Alice, knows, x,
                                        x, age, u, JenaSPARQLFilter.build("?u < 23"))))
                                .add(new QueryOp(createQuery(x, knows, Bob)))
                                .add(Projection.of("u"))
                                .build(),
                       EmptyRefSet.emptySet(),
                       JoinOp.builder(
                               new QueryOp(createQuery(
                                       Alice, knows, x,
                                       x, age, u, JenaSPARQLFilter.build("?u < 23"))),
                               new QueryOp(createQuery(x, knows, Bob))
                       ).add(Projection.of("u")).build()),
                //base case with a single product
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, knows, x)))
                                .add(new QueryOp(createQuery(
                                        Alice, age, u, JenaSPARQLFilter.build("?u < 23"))))
                                .add(Projection.of("u"))
                                .build(),
                       EmptyRefSet.emptySet(),
                       CartesianOp.builder()
                               .add(new QueryOp(createQuery(Alice, knows, x)))
                               .add(new QueryOp(createQuery(
                                       Alice, age, u, JenaSPARQLFilter.build("?u < 23"))))
                               .add(Projection.of("u"))
                               .build()),
                //apply base case with join in child
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, knows, x)))
                                .add(ConjunctionOp.builder()
                                        .add(new QueryOp(createQuery(Bob, knows, y)))
                                        .add(new QueryOp(createQuery(
                                                y, age, u, JenaSPARQLFilter.build("?u < 23"))))
                                        .build())
                                .add(Projection.of("y", "u"))
                                .build(),
                       EmptyRefSet.emptySet(),
                       CartesianOp.builder()
                               .add(new QueryOp(createQuery(Alice, knows, x)))
                               .add(JoinOp.create(
                                       new QueryOp(createQuery(Bob, knows, y)),
                                       new QueryOp(createQuery
                                               (y, age, u, JenaSPARQLFilter.build("?u < 23")))))
                               .add(Projection.of("y", "u"))
                               .build()),
                // do not project-out vars used in filters
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(x, age, u)))
                                .add(new QueryOp(createQuery(x, age, v, x, name, w)))
                                .add(JenaSPARQLFilter.build("?u < ?v"))
                                .add(Projection.of("x"))
                                .build(),
                       EmptyRefSet.emptySet(),
                       JoinOp.builder(new QueryOp(createQuery(x, age, u)),
                                      new QueryOp(createQuery(x, age, v, x, name, w)))
                               .add(JenaSPARQLFilter.build("?u < ?v"))
                               .add(Projection.of("x"))
                               .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nonnull RefSet<Op> locked, @Nullable Op expected) {
        if (expected == null)
            expected = in;
        boolean equalsIn = expected.equals(in);
        JoinOrderPlanner joPlanner = DaggerTestComponent.builder().build().joinOrderPlanner();
        ConjunctionReplaceStep step = new ConjunctionReplaceStep(joPlanner);
        Op actual = step.plan(in, locked);
        assertEquals(actual, expected);
        if (equalsIn)
            assertSame(actual, in);
    }

}