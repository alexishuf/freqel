package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Limit;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.CQueryContext;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class PushLimitStepTest implements TestContext {
    private static EmptyEndpoint ep = new EmptyEndpoint();

    private static @Nonnull EndpointQueryOp q(Object... args) {
        return new EndpointQueryOp(ep, CQueryContext.createQuery(args));
    }

    @DataProvider
    public static @Nonnull Object[][] testData() {
        EndpointQueryOp xKnowsY = q(x, knows, y,
                                    y, age, u,
                                    Distinct.INSTANCE, SPARQLFilter.build("?u > 23"));
        EndpointQueryOp xKnowsY23 = q(x, knows, y,
                                      y, age, u,
                                      Distinct.INSTANCE, SPARQLFilter.build("?u > 23"), Limit.of(23));
        PipeOp xKnowsYPipe23 = new PipeOp(xKnowsY);
        xKnowsYPipe23.modifiers().add(Limit.of(23));
        return Stream.of(
                // singleton trees with no effect
                asList(q(Alice, knows, x), emptySet(), null),
                asList(q(Alice, knows, x), singleton(xKnowsY), null),
                asList(q(Alice, knows, x, Limit.of(23)), emptySet(), null),
                asList(xKnowsY, singleton(xKnowsY), null),
                asList(xKnowsY23, singleton(xKnowsY23), null),
                // do not push into join
                asList(JoinOp.builder(xKnowsY, q(y, knows, Alice))
                                .add(Limit.of(23)).build(),
                       emptySet(), null),
                // push into union
                asList(UnionOp.builder()
                                .add(xKnowsY)
                                .add(q(y, knows, Alice))
                                .add(Limit.of(23)).build(),
                       emptySet(),
                       UnionOp.builder()
                               .add(xKnowsY23)
                               .add(q(y, knows, Alice, Limit.of(23)))
                               .add(Limit.of(23)).build()
                ),
                //push into cartesian
                asList(CartesianOp.builder()
                                .add(xKnowsY)
                                .add(q(Alice, knows, z))
                                .add(Limit.of(23)).add(Projection.of("y", "z")).build(),
                       emptySet(),
                       CartesianOp.builder()
                               .add(xKnowsY23)
                               .add(q(Alice, knows, z, Limit.of(23)))
                               .add(Limit.of(23)).add(Projection.of("y", "z")).build()),
                //add pipe if shared
                asList(UnionOp.builder().add(xKnowsY)
                                        .add(q(Alice, knows, y))
                                        .add(Projection.of("y")).add(Limit.of(23)).build(),
                       singleton(xKnowsY),
                       UnionOp.builder()
                               .add(xKnowsYPipe23)
                               .add(q(Alice, knows, y, Limit.of(23)))
                               .add(Projection.of("y")).add(Limit.of(23)).build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nonnull Set<RefEquals<Op>> shared, @Nullable Op expected) {
        if (expected == null)
            expected = in;
        boolean expectSame = expected == in;
        expected = TreeUtils.deepCopy(expected);
        Op actual = new PushLimitStep().plan(in, shared);
        assertEquals(actual, expected);
        if (expectSame)
            assertSame(actual, in);
    }
}