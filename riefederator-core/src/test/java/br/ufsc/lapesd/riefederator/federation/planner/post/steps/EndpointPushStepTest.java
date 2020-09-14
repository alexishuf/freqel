package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.DisjunctiveProfile;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLDisjunctiveProfile;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.EmptyRefSet;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class EndpointPushStepTest implements TestContext {
    private static Model emptyModel = ModelFactory.createDefaultModel();

    private static DQEndpoint dq1 = ARQEndpoint.forModel(ModelFactory.createDefaultModel());
    private static DQEndpoint dq1NO = new ARQEndpoint(
            "dq1NO", q -> QueryExecutionFactory.create(q, emptyModel),
            null, () -> {}, true) {
        @Override public @Nonnull DisjunctiveProfile getDisjunctiveProfile() {
            return new SPARQLDisjunctiveProfile().forbidOptional();
        }
    };
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
                       null),
                // wrap cartesian
                asList(CartesianOp.builder()
                                .add(q(dq1, Alice, knows, y, y, age, u))
                                .add(q(dq1, Alice, age, v))
                                .add(SPARQLFilter.build("?y < ?u")).build(),
                       new DQueryOp(dq1, CartesianOp.builder()
                               .add(q(dq1, Alice, knows, y, y, age, u))
                               .add(q(dq1, Alice, age, v))
                               .add(SPARQLFilter.build("?y < ?u")).build())),
                // do not wrap unfiltered cartesian root
                asList(CartesianOp.builder()
                                .add(q(dq1, Alice, knows, y, y, age, u))
                                .add(q(dq1, Alice, age, v)).build(),
                        null),
                // wrap unfiltered cartesian child
                asList(JoinOp.create(CartesianOp.builder()
                                        .add(q(dq1, Alice, knows, x))
                                        .add(q(dq1, Alice, knows, y)).build(),
                                     q(dq1, x, knows, y)),
                       new DQueryOp(dq1,
                               JoinOp.create(CartesianOp.builder()
                                               .add(q(dq1, Alice, knows, x))
                                               .add(q(dq1, Alice, knows, y)).build(),
                                             q(dq1, x, knows, y)))),
                // wrap optional
                asList(JoinOp.create(q(dq1, Alice, knows, x), q(dq1, x, age, u, Optional.EXPLICIT)),
                       new DQueryOp(dq1, JoinOp.create(q(dq1, Alice, knows, x),
                                                       q(dq1, x, age, u, Optional.EXPLICIT)))),
                // do not wrap optional if forbidden
                asList(JoinOp.create(q(dq1NO, Alice, knows, x),
                                     q(dq1NO, x, age, u, Optional.EXPLICIT)),
                       null),
                // wrap optional root even if forbidden
                asList(UnionOp.builder().add(q(dq1NO, x, knows, Alice))
                                        .add(q(dq1NO, x, knows, Bob))
                                        .add(Optional.EXPLICIT).build(),
                       new DQueryOp(dq1NO, UnionOp.builder().add(q(dq1NO, x, knows, Alice))
                                                            .add(q(dq1NO, x, knows, Bob))
                                                            .add(Optional.EXPLICIT).build()))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nullable Op expected) {
        if (expected == null)
            expected = in;
        boolean expectSame = expected == in;
        expected = TreeUtils.deepCopy(expected);
        Op actual = new EndpointPushStep().plan(in, EmptyRefSet.emptySet());

        assertEquals(actual, expected);
        if (expectSame)
            assertSame(actual, in);
    }

}