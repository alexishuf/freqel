package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import org.apache.jena.rdf.model.ModelFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class EndpointPushStepTest implements TestContext {
    private static DQEndpoint dq1 = ARQEndpoint.forModel(ModelFactory.createDefaultModel());
    private static CQEndpoint cq1 = new EmptyEndpoint();

    private static @Nonnull EndpointQueryOp q(@Nonnull TPEndpoint endpoint, Object... args) {
        return new EndpointQueryOp(endpoint, createQuery(args));
    }

    @DataProvider
    public @Nonnull Object[][] testData() {
        return Stream.of(
                // not a DQEndpoint, no change
                asList(new EndpointQueryOp(cq1, createQuery(x, knows, y)), null),
                // is a DQEndpoint but is trivial
                asList(new EndpointQueryOp(dq1, createQuery(x, knows, y)), null),
                // same endpoint but is not DQEndpoint
                asList(JoinOp.create(q(cq1, x, knows, y), q(cq1, y, knows, Bob)),
                       null),
                // wrap
                asList(JoinOp.create(q(dq1, x, knows, y), q(dq1, y, knows, Bob)),
                       new DQueryOp(dq1, JoinOp.create(q(dq1, x, knows, y), q(dq1, y, knows, Bob)))),
                // wrap child of non-wrappable node
                asList(UnionOp.builder()
                                .add(JoinOp.create(q(dq1, x, knows, y), q(dq1, y, knows, Bob)))
                                .add(q(cq1, x, age, u)).build(),
                        UnionOp.builder()
                                .add(new DQueryOp(dq1, JoinOp.create(q(dq1, x, knows, y), q(dq1, y, knows, Bob))))
                                .add(q(cq1, x, age, u)).build()),
                //trivial child stops wrapping of parent
                asList(UnionOp.builder()
                                .add(q(cq1, x, knows, Alice))
                                .add(q(dq1, x, knows, Bob)).build(), null),
                // same as above: child ordering has no effect
                asList(UnionOp.builder()
                                .add(q(dq1, x, knows, Alice))
                                .add(q(cq1, x, knows, Bob)).build(), null),
                // non-trivial op stops wrapping of parent
                asList(UnionOp.builder()
                                .add(JoinOp.create(q(dq1, Alice, knows, x), q(cq1, x, knows, y)))
                                .add(q(dq1, y, age, u)).build(),
                       null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nullable Op expected) {
        if (expected == null)
            expected = in;
        boolean expectSame = expected == in;
        expected = TreeUtils.deepCopy(expected);
        Op actual = new EndpointPushStep().plan(in, Collections.emptySet());

        assertEquals(actual, expected);
        if (expectSame)
            assertSame(actual, in);
    }

}