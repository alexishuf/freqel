package br.ufsc.lapesd.freqel.federation.planner;

import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.federation.SimpleFederationModule;
import br.ufsc.lapesd.freqel.federation.SingletonSourceFederation;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class PostPlannerTest implements TestContext {
    private static final CQEndpoint e1 = new EmptyEndpoint(), e2 = new EmptyEndpoint();

    public static @Nonnull List<Provider<? extends PostPlanner>> providers = singletonList(
            new NamedSupplier<>("default",
                    () -> SingletonSourceFederation.getInjector().getInstance(PostPlanner.class))
    );

    public static @Nonnull List<Class<? extends Provider<? extends PostPlanner>>> providerClasses
            = singletonList(SimpleFederationModule.DefaultPostPlannerProvider.class);

    @DataProvider public static Object[][] exactTreeData() {
        EndpointQueryOp query1 = new EndpointQueryOp(e2, createQuery(x, age, u));
        EndpointQueryOp query1f = new EndpointQueryOp(e2,
                createQuery(x, age, u, JenaSPARQLFilter.build("?u > 23")));
        PipeOp pipe1 = new PipeOp(query1), pipe2 = new PipeOp(query1);
        pipe1.modifiers().add(JenaSPARQLFilter.build("?u > 23"));
        pipe2.modifiers().add(JenaSPARQLFilter.build("?u > 23"));

        return Stream.of(
                //single query node, no effect
                asList(new EndpointQueryOp(e1, createQuery(Alice, knows, x)), null),
                //erase PipeOps that share child and modifiers
                asList(UnionOp.builder()
                                .add(JoinOp.create(
                                        pipe1,
                                        new EndpointQueryOp(e1, createQuery(x, knows, Alice))))
                                .add(JoinOp.create(
                                        pipe2,
                                        new EndpointQueryOp(e1, createQuery(x, knows, Bob))))
                                .build(),
                       UnionOp.builder()
                               .add(JoinOp.create(
                                       query1f,
                                       new EndpointQueryOp(e1, createQuery(x, knows, Alice))))
                               .add(JoinOp.create(
                                       query1f,
                                       new EndpointQueryOp(e1, createQuery(x, knows, Bob))))
                               .build())
        ).flatMap(l -> providers.stream().map(p -> {
            ArrayList<Object> copy = new ArrayList<>(l);
            copy.add(0, p);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "exactTreeData")
    public void testExactTree(@Nonnull Supplier<PostPlanner> postPlannerSupplier,
                              @Nonnull Op in, @Nullable Op expected) {
        PostPlanner postPlanner = postPlannerSupplier.get();
        if (expected == null)
            expected = in;
        boolean expectSame = expected == in;
        if (expectSame)
            expected = TreeUtils.deepCopy(in);
        Op actual = postPlanner.plan(in);
        if (expectSame)
            assertSame(actual, in);
        assertEquals(actual, expected);
    }
}