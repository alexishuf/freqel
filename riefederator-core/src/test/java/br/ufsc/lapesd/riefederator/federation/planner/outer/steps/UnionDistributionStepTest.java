package br.ufsc.lapesd.riefederator.federation.planner.outer.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
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
public class UnionDistributionStepTest implements TestContext {
    @DataProvider
    public static Object[][] testData() {
        QueryOp q1 = new QueryOp(createQuery(Alice, knows, x));
        Op u1 = UnionOp.build(asList(q1, new QueryOp(createQuery(Bob, knows, x))));
        return Stream.of(
                asList(q1, emptySet(), q1),
                asList(q1, Collections.singleton(q1), q1),
                asList(u1, emptySet(), u1),
                // base replacement scenario
                asList(ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(
                                                y, knows, x,
                                                y, age, u, SPARQLFilter.build("?u > 23"))))
                                        .add(new QueryOp(createQuery(Bob, knows, x)))
                                        .add(Distinct.INSTANCE)
                                        .build())
                                .add(new QueryOp(createQuery(
                                        x, age, v, SPARQLFilter.build("?v < 23"))))
                                .build(),
                       emptySet(),
                       UnionOp.builder()
                               .add(new QueryOp(createQuery(
                                       y, knows, x,
                                       y, age, u, SPARQLFilter.build("?u > 23"),
                                       x, age, v, SPARQLFilter.build("?v < 23"))))
                               .add(new QueryOp(createQuery(
                                       Bob, knows, x,
                                       x, age, v, SPARQLFilter.build("?v < 23"))))
                               .add(Distinct.INSTANCE)
                               .build()),
                // locked stops rewrite
                asList(ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(q1)
                                        .add(new QueryOp(createQuery(Bob, knows, x)))
                                        .add(Distinct.INSTANCE)
                                        .build())
                                .add(new QueryOp(createQuery(x, age, u)))
                                .build(),
                       singleton(q1),
                       null),
                // extra node is preserved
                asList(ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(
                                                Alice, knows, x,
                                                x, age, u, SPARQLFilter.build("?u < 23"))))
                                        .add(new QueryOp(createQuery(
                                                Bob, knows, x,
                                                x, age, u, SPARQLFilter.build("?u < 27"))))
                                        .add(Distinct.INSTANCE)
                                        .build())
                                .add(new QueryOp(createQuery(
                                        x, knows, y,
                                        y, age, v, SPARQLFilter.build("?v > 7"))))
                                .add(CartesianOp.builder()
                                        .add(new QueryOp(createQuery(y, name, z)))
                                        .add(new QueryOp(createQuery(Alice, knows, Bob)))
                                        .build())
                                .build(),
                       Sets.newHashSet(q1, u1),
                       ConjunctionOp.builder()
                               .add(CartesianOp.builder()
                                       .add(new QueryOp(createQuery(y, name, z)))
                                       .add(new QueryOp(createQuery(Alice, knows, Bob)))
                                       .build())
                               .add(UnionOp.builder()
                                       .add(new QueryOp(createQuery(
                                               Alice, knows, x,
                                               x, age, u, SPARQLFilter.build("?u < 23"),
                                               x, knows, y,
                                               y, age, v, SPARQLFilter.build("?v > 7"))))
                                       .add(new QueryOp(createQuery(
                                               Bob, knows, x,
                                               x, age, u, SPARQLFilter.build("?u < 27"),
                                               x, knows, y,
                                               y, age, v, SPARQLFilter.build("?v > 7"))))
                                       .add(Distinct.INSTANCE)
                                       .build())
                               .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nonnull Set<RefEquals<Op>> locked, @Nullable Op expected) {
        if (expected == null)
            expected = in;
        boolean expectSame = expected.equals(in);
        UnionDistributionStep step = new UnionDistributionStep();
        Op actual = step.visit(in, locked);
        assertEquals(actual, expected);
        if (expectSame)
            assertSame(actual, in);
    }
}