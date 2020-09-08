package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.CQueryContext;
import org.apache.jena.sparql.expr.Expr;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;

public class DefaultFilterJoinPlannerTest implements TestContext {
    private static final EmptyEndpoint ep = new EmptyEndpoint();

    private static @Nonnull EndpointQueryOp q(Object... args) {
        return new EndpointQueryOp(ep, CQueryContext.createQuery(args));
    }

    private static @Nonnull BitSet b(int... indices) {
        BitSet bs = new BitSet();
        for (int index : indices)
            bs.set(index);
        return bs;
    }

    public static @Nonnull DefaultFilterJoinPlanner createDefault() {
        NoOpPerformanceListener listener = NoOpPerformanceListener.INSTANCE;
        ArbitraryJoinOrderPlanner joinOrderPlanner = new ArbitraryJoinOrderPlanner(listener);
        ThresholdCardinalityComparator cardComparator = ThresholdCardinalityComparator.DEFAULT;
        return new DefaultFilterJoinPlanner(cardComparator, joinOrderPlanner);
    }

    @DataProvider
    public static @Nonnull Object[][] testHasJoinByComponentData() {
        return Stream.of(
                asList(singleton("x"), "?x < ?y", true),
                asList(singleton("z"), "?x < ?y", false),
                asList(newHashSet("x", "y"), "?x < ?y", false),
                asList(newHashSet("x1", "y1"), "?x1 < ?x2 || ?y1 < ?y2", true),
                asList(newHashSet("x1", "x2", "y1"), "?x1 < ?x2 || ?y1 < ?y2", true),
                asList(newHashSet("x1", "x2", "y1"), "?x1 < ?x2 && ?y1 < ?y2", true),
                asList(newHashSet("x1", "x2", "y1"), "?x1 < ?x2 && !(?y1 < ?y2)", true),
                asList(emptySet(), "?x1 < ?x2 && !(?y1 < ?y2)", false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testHasJoinByComponentData")
    public void testHasJoinByComponent(@Nonnull Set<String> nodeVars, @Nonnull String exprString,
                                       boolean expected) {
        Expr expr = SPARQLFilter.build(exprString).getExpr();
        boolean actual = DefaultFilterJoinPlanner.hasJoinByComponent(nodeVars, expr);
        assertEquals(actual, expected);
    }

    @DataProvider
    public static @Nonnull Object[][] testComponentsData() {
        return Stream.of(
                asList(singleton("?x < ?y"), singletonList("?x < ?y")),
                asList(singleton("?x < ?y && ?u > ?v"), asList("?x < ?y", "?u > ?v")),
                asList(singleton("?x > ?y"), singletonList("?x > ?y")),
                asList(singleton("?x > ?y && ?y > ?z"), asList("?x > ?y", "?y > ?z")),
                asList(singleton("?x > ?y && !(?y > ?z)"), asList("?x > ?y", "!(?y > ?z)")),
                asList(singleton("?x > ?y || ?y > ?z"), asList("?x > ?y || ?y > ?z"))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testComponentsData")
    public void testComponents(@Nonnull Collection<String> in,
                               @Nonnull Collection<String> expectedStrings) {
        DefaultFilterJoinPlanner def = createDefault();
        List<Op> nodes = asList(new EmptyOp(singleton("x")), new EmptyOp(singleton("y")));
        DefaultFilterJoinPlanner.State state = def.createState(nodes, emptySet());
        for (String filterString : in)
            state.addComponents(SPARQLFilter.build(filterString));

        List<SPARQLFilter> actual = state.getComponents();
        HashSet<SPARQLFilter> set = new HashSet<>(actual);
        assertEquals(actual.size(), set.size());

        Set<SPARQLFilter> expected = new HashSet<>();
        for (String string : expectedStrings)
            expected.add(SPARQLFilter.build(string));
        assertEquals(set, new HashSet<>(expected));
    }

    @DataProvider
    public static @Nonnull Object[][] testBestNodeIndexData() {
        return Stream.of(
                // chose last
                asList(asList(q(Alice, age, u), q(Alice, age, v)), b(0, 1), b(), 1),
                asList(asList(q(Alice, age, u), q(Alice, age, v)), b(1), b(), 1),
                asList(asList(q(Alice, age, u), q(Alice, age, v)), b(0), b(), 0),
                // only chose from subset
                asList(asList(q(Alice, age, u), q(Alice, age, v, SPARQLFilter.build("?v < ?w"))),
                       b(0), b(1), 0),
                // already modified op has preference
                asList(asList(q(Alice, age, u), q(Alice, age, v)), b(0, 1), b(0), 0),
                // op with inputs has preference
                asList(asList(q(Alice, age, u, SPARQLFilter.build("?u > ?w")),
                              q(Alice, age, v)),
                       b(0, 1), b(), 0)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testBestNodeIndexData")
    public void testBestNodeIndex(@Nonnull List<Op> nodes, @Nonnull BitSet subset,
                                  @Nonnull BitSet hot, int expectedIndex) {
        int actual = DefaultFilterJoinPlanner.bestNodeIndex(nodes, subset, hot);
        assertEquals(actual, expectedIndex);
    }

}