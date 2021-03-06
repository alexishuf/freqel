package br.ufsc.lapesd.freqel.federation.planner.post.steps;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.parse.CQueryContext;
import br.ufsc.lapesd.freqel.util.ref.EmptyRefSet;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import br.ufsc.lapesd.freqel.util.ref.RefSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class PushLimitStepTest implements TestContext {
    private static final EmptyEndpoint ep = new EmptyEndpoint();

    private static @Nonnull EndpointQueryOp q(Object... args) {
        return new EndpointQueryOp(ep, CQueryContext.createQuery(args));
    }

    @DataProvider
    public static @Nonnull Object[][] testData() {
        EndpointQueryOp xKnowsY = q(x, knows, y,
                                    y, age, u,
                                    Distinct.INSTANCE, JenaSPARQLFilter.build("?u > 23"));
        EndpointQueryOp xKnowsY23 = q(x, knows, y,
                                      y, age, u,
                                      Distinct.INSTANCE, JenaSPARQLFilter.build("?u > 23"), Limit.of(23));
        PipeOp xKnowsYPipe23 = new PipeOp(xKnowsY);
        xKnowsYPipe23.modifiers().add(Limit.of(23));
        return Stream.of(
                // singleton trees with no effect
                asList(q(Alice, knows, x), EmptyRefSet.emptySet(), null),
                asList(q(Alice, knows, x), IdentityHashSet.of(xKnowsY), null),
                asList(q(Alice, knows, x, Limit.of(23)), EmptyRefSet.emptySet(), null),
                asList(xKnowsY, IdentityHashSet.of(xKnowsY), null),
                asList(xKnowsY23, IdentityHashSet.of(xKnowsY23), null),
                // do not push into join
                asList(JoinOp.builder(xKnowsY, q(y, knows, Alice))
                                .add(Limit.of(23)).build(),
                       EmptyRefSet.emptySet(), null),
                // push into union
                asList(UnionOp.builder()
                                .add(xKnowsY)
                                .add(q(y, knows, Alice))
                                .add(Limit.of(23)).build(),
                       EmptyRefSet.emptySet(),
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
                       EmptyRefSet.emptySet(),
                       CartesianOp.builder()
                               .add(xKnowsY23)
                               .add(q(Alice, knows, z, Limit.of(23)))
                               .add(Limit.of(23)).add(Projection.of("y", "z")).build()),
                //add pipe if shared
                asList(UnionOp.builder().add(xKnowsY)
                                        .add(q(Alice, knows, y))
                                        .add(Projection.of("y")).add(Limit.of(23)).build(),
                       IdentityHashSet.of(xKnowsY),
                       UnionOp.builder()
                               .add(xKnowsYPipe23)
                               .add(q(Alice, knows, y, Limit.of(23)))
                               .add(Projection.of("y")).add(Limit.of(23)).build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nonnull RefSet<Op> shared, @Nullable Op expected) {
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