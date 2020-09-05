package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.parse.CQueryContext;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.apache.jena.rdf.model.ModelFactory;
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
public class PushDistinctStepTest implements TestContext {
    private static final CQEndpoint ep1 = ARQEndpoint.forModel(ModelFactory.createDefaultModel());

    private static @Nonnull EndpointQueryOp q(Object... args) {
        return new EndpointQueryOp(ep1, CQueryContext.createQuery(args));
    }

    @DataProvider
    public static Object[][] testData() {
        EndpointQueryOp xKnowsAlice = q(x, knows, Alice);
        PipeOp xKnowsAlicePipe = new PipeOp(xKnowsAlice);
        xKnowsAlicePipe.modifiers().add(Distinct.INSTANCE);
        EndpointQueryOp xKnowsAliceDistinct = q(x, knows, Alice, Distinct.INSTANCE);
        return Stream.of(
                asList(q(x, knows, y), null, emptySet()),
                asList(q(x, knows, y, Distinct.INSTANCE), null, emptySet()),
                asList(xKnowsAlice, null, singleton(RefEquals.of(xKnowsAlice))),
                asList(xKnowsAliceDistinct, null, singleton(RefEquals.of(xKnowsAliceDistinct))),
                // no change with union
                asList(UnionOp.builder()
                                .add(q(x, knows, Alice))
                                .add(q(x, knows, Bob))
                                .build(),
                       null, emptySet()),
                // push distinct down union
                asList(UnionOp.builder()
                                .add(q(x, knows, Alice))
                                .add(q(x, knows, Bob))
                                .add(Distinct.INSTANCE).build(),
                       UnionOp.builder()
                               .add(q(x, knows, Alice, Distinct.INSTANCE))
                               .add(q(x, knows, Bob, Distinct.INSTANCE))
                               .add(Distinct.INSTANCE).build(),
                       emptySet()),
                // add pipe while pushing down Join
                asList(JoinOp.builder(xKnowsAlice, q(x, age, u))
                                .add(Distinct.INSTANCE).build(),
                       JoinOp.builder(xKnowsAlicePipe, q(x, age, u, Distinct.INSTANCE))
                               .add(Distinct.INSTANCE).build(),
                       singleton(RefEquals.of(xKnowsAlice))),
                // do not add Distinct to intermediate nodes
                asList(JoinOp.builder(UnionOp.builder()
                                                .add(q(x, knows, Alice))
                                                .add(q(x, knows, Bob)).build(),
                                      q(x, age, u)).add(Distinct.INSTANCE).build(),
                       JoinOp.builder(UnionOp.builder()
                                               .add(q(x, knows, Alice, Distinct.INSTANCE))
                                               .add(q(x, knows, Bob, Distinct.INSTANCE)).build(),
                                       q(x, age, u, Distinct.INSTANCE)
                       ).add(Distinct.INSTANCE).build(),
                       emptySet())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nullable Op expected,
                     @Nonnull Set<RefEquals<Op>> shared) {
        if (expected == null)
            expected = in;
        expected = TreeUtils.deepCopy(expected);
        boolean expectSame = expected == in;
        Op actual = new PushDistinctStep().plan(in, shared);
        assertEquals(actual, expected);
        if (expectSame)
            assertSame(actual, in);
    }
}