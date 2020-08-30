package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class CartesianIntroductionStepTest implements TestContext {

    @DataProvider
    public static Object[][] cartesianComponentsData() {
        return Stream.of(
                asList(singleton(new Triple(Alice, p1, x)),
                        singleton(singleton(new Triple(Alice, p1, x)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(y, p2, Bob)),
                        asList(singleton(new Triple(Alice, p1, x)),
                                singleton(new Triple(y, p2, Bob)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Alice)),
                        asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y)),
                                singleton(new Triple(z, p3, Alice)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob)),
                        asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y)),
                                singleton(new Triple(z, p3, Bob)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob), new Triple(z, p4, Bob)),
                        asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y)),
                                asList(new Triple(z, p3, Bob), new Triple(z, p4, Bob)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob), new Triple(z, p4, y)),
                        singleton(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                                new Triple(z, p3, Bob), new Triple(z, p4, y)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                        new Triple(z, p3, Bob), new Triple(z, p4, x)),
                        singleton(asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                                new Triple(z, p3, Bob), new Triple(z, p4, x)))),
                asList(asList(new Triple(Alice, p1, x), new Triple(Alice, p2, y)),
                        asList(singleton(new Triple(Alice, p1, x)),
                                singleton(new Triple(Alice, p2, y))))
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "cartesianComponentsData")
    public void testCartesianComponents(Collection<Triple> triples,
                                        Collection<Collection<Triple>> expected) {
        CartesianIntroductionStep step = new CartesianIntroductionStep();
        List<IndexedSet<Triple>> list = step.getCartesianComponents(IndexedSet.from(triples));
        assertEquals(new HashSet<>(list), expected.stream().map(IndexedSet::from).collect(toSet()));

        assertEquals(MutableCQuery.from(triples).attr().isJoinConnected(), expected.size()==1);
    }


    @DataProvider
    public static @Nonnull Object[][] testData() {
        SPARQLParser parser = SPARQLParser.strict();
        String prolog = "PREFIX : <"+EX+">\n";
        return Stream.of(
                //no change
                asList(new QueryOp(createQuery(Alice, knows, x)), null),
                // two completely unrelated triple patterns
                asList(new QueryOp(createQuery(Alice, knows, x, Bob, knows, y)),
                       CartesianOp.builder()
                               .add(new QueryOp(createQuery(Alice, knows, x)))
                               .add(new QueryOp(createQuery(Bob, knows, y)))
                               .build()),
                // cartesian since joining by constant
                asList(new QueryOp(createQuery(
                                Alice, knows, x,
                                Alice, age, y)),
                       CartesianOp.builder()
                                .add(new QueryOp(createQuery(Alice, knows, x)))
                                .add(new QueryOp(createQuery(Alice, age, y)))
                                .build()),
                // cartesian introduction in child
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, p1, x)))
                                .add(new QueryOp(createQuery(Bob, p1, x, Bob, p2, y)))
                                .build(),
                       ConjunctionOp.builder()
                               .add(new QueryOp(createQuery(Alice, p1, x)))
                               .add(CartesianOp.builder()
                                       .add(new QueryOp(createQuery(Bob, p1, x)))
                                       .add(new QueryOp(createQuery(Bob, p2, y)))
                                       .build())
                               .build()),
                // cartesian introduction preserves modifier
                asList(ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, p1, x)))
                                .add(new QueryOp(createQuery(
                                        Bob, p1, x,
                                        Bob, p2, y, Optional.INSTANCE)))
                                .build(),
                        ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(Alice, p1, x)))
                                .add(CartesianOp.builder()
                                        .add(new QueryOp(createQuery(Bob, p1, x)))
                                        .add(new QueryOp(createQuery(Bob, p2, y)))
                                        .add(Optional.INSTANCE)
                                        .build())
                                .build())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nullable Op expected) {
        if (expected == null)
            expected = TreeUtils.deepCopy(in);
        boolean isSame = expected.equals(in);
        CartesianIntroductionStep step = new CartesianIntroductionStep();
        Op actual = step.plan(in, emptySet());
        assertEquals(actual, expected);
        if (isSame)
            assertSame(actual, in);
    }

}