package br.ufsc.lapesd.riefederator.federation.planner.conjunctive;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.RelativeCardinalityAdder;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.cardinality.JoinCardinalityEstimator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.*;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.webapis.EmptyWebApiEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.Cardinality.*;
import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.streamPreOrder;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class GreedyJoinOrderConjunctivePlannerTest implements TestContext {
    private static final Var t = new StdVar("t");
    private static final Var b = new StdVar("b");
    private static final Var k = new StdVar("k");
    private static final Var d = new StdVar("d");
    private static final Var m = new StdVar("m");
    private static final Var c = new StdVar("c");
    private static final Var e = new StdVar("e");

    private static final EmptyEndpoint ep1 = new EmptyEndpoint(), ep2 = new EmptyEndpoint();
    private static final EmptyWebApiEndpoint wep = new EmptyWebApiEndpoint();

    static {
        ep1.addAlternative(ep2);
    }

    private static @Nonnull GreedyJoinOrderPlanner.Data createData(@Nonnull JoinGraph graph) {
        GreedyJoinOrderPlanner planner = new GreedyJoinOrderPlanner(
                NoOpPerformanceListener.INSTANCE, NoCardinalityEnsemble.INSTANCE,
                RelativeCardinalityAdder.DEFAULT,
                new AverageJoinCardinalityEstimator(ThresholdCardinalityComparator.DEFAULT));
        return planner.createData(graph);
    }

    @DataProvider
    public static @Nonnull Object[][] joinCardinalityEstimatorData() {
        return Stream.of(
                new AverageJoinCardinalityEstimator(ThresholdCardinalityComparator.DEFAULT),
                new BindJoinCardinalityEstimator(
                        new FixedCardinalityEnsemble(GeneralSelectivityHeuristic.DEFAULT))
        ).map(e -> new Object[] {e}).toArray(Object[][]::new);
    }

    private static @Nonnull EndpointQueryOp n(@Nonnull CQEndpoint ep, Cardinality cardinality,
                                              List<Var> inputs, Term... terms) {
        MutableCQuery q = new MutableCQuery();
        for (int i = 0; i < terms.length; i +=3)
            q.add(new Triple(terms[i], terms[i+1], terms[i+2]));
        int i = 0;
        for (Var input : inputs)
            q.annotate(input, AtomInputAnnotation.asRequired(new Atom("Atom-" + i), "Atom-" + i).get());
        EndpointQueryOp op = new EndpointQueryOp(ep, q);
        op.setCardinality(cardinality);
        return op;
    }
    private static @Nonnull EndpointQueryOp n(@Nonnull CQEndpoint ep, Cardinality cardinality,
                                              Term... terms) {
        return n(ep, cardinality, emptyList(), terms);
    }
    private static @Nonnull Op m(EndpointQueryOp... nodes) {
        UnionOp.Builder b = UnionOp.builder();
        for (EndpointQueryOp node : nodes) b.add(node);
        return b.build();
    }

    @DataProvider
    public static Object[][] compareTupleData() {
        return Stream.of(
                // trivial equals
                asList(Cardinality.UNSUPPORTED, 0, false,
                       Cardinality.UNSUPPORTED, 0, false, 0),
                asList(Cardinality.EMPTY, 0, false,
                       Cardinality.EMPTY, 0, false, 0),
                asList(NON_EMPTY, 0, false,
                       NON_EMPTY, 0, false, 0),
                asList(Cardinality.guess(3), 0, false,
                       Cardinality.guess(3), 0, false, 0),
                asList(Cardinality.lowerBound(3), 0, false,
                       Cardinality.lowerBound(3), 0, false, 0),
                asList(upperBound(3), 0, false,
                       upperBound(3), 0, false, 0),
                asList(exact(3), 0, false,
                       exact(3), 0, false, 0),

                // hierarchy of reliability (UNSUPPORTED)
                asList(Cardinality.UNSUPPORTED, 0, false,
                       Cardinality.guess(2), 0, false, 1),
                asList(Cardinality.UNSUPPORTED, 0, false,
                       Cardinality.lowerBound(70), 0, false, 1),
                asList(Cardinality.UNSUPPORTED, 0, false,
                       upperBound(70), 0, false, 1),
                asList(Cardinality.UNSUPPORTED, 0, false,
                       exact(70), 0, false, 1),

                // prefer UNSUPPORTED if other side is guessed or known to be huge
                asList(Cardinality.UNSUPPORTED, 0, false,
                       exact(10800), 0, false, -1),
                asList(Cardinality.UNSUPPORTED, 0, false,
                       Cardinality.guess(10800), 0, false, -1),

                // hierarchy of reliability (GUESS)
                asList(Cardinality.guess(2), 0, false,
                       exact(50), 0, false, 1),
                asList(Cardinality.guess(2), 0, false,
                       upperBound(50), 0, false, 1),
                asList(NON_EMPTY, 0, false,
                       Cardinality.exact(2), 0, false, 1),

                // comparison between NON_EMPTY, GUESS and LOWER_BOUND has a tolerance
                asList(NON_EMPTY, 0, false,
                       Cardinality.guess(50), 0, false, 1), //guess(50) is more reliable
                asList(NON_EMPTY, 0, false,
                       Cardinality.guess(16), 0, false, 1), //guess(16) is more reliable
                asList(NON_EMPTY, 0, false,
                       Cardinality.lowerBound(50), 0, false, 1), //lowerBound(50) is more reliable
                asList(Cardinality.guess(2), 0, false,
                       Cardinality.lowerBound(50), 0, false, 1), //lowerBound(50) is more reliable
                asList(Cardinality.guess(2), 0, false,
                       Cardinality.lowerBound(80), 0, false, -1), //out of tolerance
                asList(Cardinality.guess(2), 0, false,
                       Cardinality.guess(6), 0, false, 0), //same reliability, but too close
                asList(Cardinality.guess(2), 0, false,
                       Cardinality.guess(50), 0, false, -1), //same reliability, compare values
                asList(Cardinality.lowerBound(2), 0, false,
                       Cardinality.lowerBound(6), 0, false, 0), //same reliability, but too close
                asList(Cardinality.lowerBound(2), 0, false,
                       Cardinality.lowerBound(50), 0, false, -1), //same reliability, compare values
                asList(Cardinality.guess(2), 0, false,
                       Cardinality.guess(80), 0, false, -1), //out of tolerance

                // UPPER_BOUND and EXACT are treated as the same, no tolerance
                asList(upperBound(5), 0, false,
                       exact(5), 0, false, 0),
                asList(upperBound(6), 0, false,
                       exact(5), 0, false, 1),
                asList(upperBound(5), 0, false,
                       exact(6), 0, false, -1),

                // smaller pending inputs wins
                asList(Cardinality.UNSUPPORTED, 0, false,
                       Cardinality.UNSUPPORTED, 1, false, -1),
                asList(Cardinality.guess(2), 0, false,
                       Cardinality.guess(1), 1, false, -1),
                asList(Cardinality.guess(3), 2, false,
                       Cardinality.guess(3), 3, false, -1),

                // web api wins
                asList(Cardinality.UNSUPPORTED, 0, true,
                       Cardinality.UNSUPPORTED, 0, false, -1),
                asList(Cardinality.guess(1), 3, true,
                       Cardinality.guess(7), 3, false, -1),
                asList(Cardinality.lowerBound(1), 3, true,
                       Cardinality.lowerBound(7), 3, false, -1)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "compareTupleData")
    public void testCompareTuple(Cardinality lCard, int lPending, boolean lApi,
                                 Cardinality rCard, int rPending, boolean rApi,
                                 int expected) {
        GreedyJoinOrderPlanner.OrderTuple l, r;
        l = new GreedyJoinOrderPlanner.OrderTuple(lCard, lPending, lApi);
        r = new GreedyJoinOrderPlanner.OrderTuple(rCard, rPending, rApi);

        assertEquals(l.compareTo(r), expected);
        assertEquals(r.compareTo(l), -1 * expected);
        if (expected != 0)
            assertNotEquals(l, r);
    }

    @SuppressWarnings({"EqualsWithItself", "SimplifiedTestNGAssertion", "SelfComparison"})
    @Test(dataProvider = "compareTupleData")
    public void testReflexiveComparison(Cardinality lCard, int lPending, boolean lApi,
                                        Cardinality rCard, int rPending, boolean rApi,
                                        int ignored) {
        GreedyJoinOrderPlanner.OrderTuple l, r;
        l = new GreedyJoinOrderPlanner.OrderTuple(lCard, lPending, lApi);
        r = new GreedyJoinOrderPlanner.OrderTuple(rCard, rPending, rApi);

        assertEquals(l.compareTo(l), 0);
        assertEquals(r.compareTo(r), 0);
        assertTrue(l.equals(l));
        assertTrue(r.equals(r));
    }

    @Test
    public void testMax() {
        List<GreedyJoinOrderPlanner.OrderTuple> list = new ArrayList<>();
        for (Object[] objects : compareTupleData()) {
            Cardinality lCard = (Cardinality) objects[0], rCard = (Cardinality) objects[3];
            int lPending = (Integer)objects[1], rPending = (Integer)objects[4];
            boolean lWebApi = (Boolean) objects[2], rWebApi = (Boolean) objects[5];
            list.add(new GreedyJoinOrderPlanner.OrderTuple(lCard, lPending, lWebApi));
            list.add(new GreedyJoinOrderPlanner.OrderTuple(rCard, rPending, rWebApi));
        }

        GreedyJoinOrderPlanner.OrderTuple max = GreedyJoinOrderPlanner.OrderTuple.MAX;
        for (GreedyJoinOrderPlanner.OrderTuple tuple : list) {
            assertEquals(tuple.compareTo(max), -1);
            assertEquals(max.compareTo(tuple),  1);
            //noinspection SimplifiedTestNGAssertion (easier to debug)
            assertFalse(tuple.equals(max));
        }
    }

    @Test(dataProvider = "joinCardinalityEstimatorData")
    public void testTakeInitialJoinSingletonGraph(@Nonnull JoinCardinalityEstimator joinCardinalityEstimator) {
        JoinGraph graph = new JoinGraph(IndexedSet.from(singletonList(
                n(ep1, NON_EMPTY, Alice, p1, x))
        ));
        GreedyJoinOrderPlanner.Data data = createData(graph);
        Op node = GreedyJoinOrderPlanner.takeInitialJoin(data, joinCardinalityEstimator);
        assertSame(node, graph.get(0));
        assertTrue(data.pending.isEmpty());
    }

    @Test(dataProvider = "joinCardinalityEstimatorData")
    public void testTakeInitialJoinSingletonGraphCleansEquivalents(@Nonnull JoinCardinalityEstimator joinCardinalityEstimator) {
        JoinGraph graph = new JoinGraph(IndexedSet.from(singletonList(
                m(n(ep1, NON_EMPTY, Alice, p1, x), n(ep2, NON_EMPTY, Alice, p1, x))
        )));
        GreedyJoinOrderPlanner.Data data = createData(graph);
        Op root = GreedyJoinOrderPlanner.takeInitialJoin(data, joinCardinalityEstimator);
        assertTrue(data.pending.isEmpty());

        assertNotSame(root, graph.get(0));
        assertTrue(graph.get(0).getChildren().contains(root));
    }


    @Test(dataProvider = "joinCardinalityEstimatorData")
    public void testTakeInitialJoinCleansEquivalents(@Nonnull JoinCardinalityEstimator joinCardinalityEstimator) {
        List<EndpointQueryOp> best = asList(
                n(ep2, guess(16), Alice, p1, x),
                n(ep2, exact(3), x, p1, y)
        );
        JoinGraph graph = new JoinGraph(IndexedSet.from(asList(
                m(n(ep1, NON_EMPTY, Alice, p1, x), best.get(0)),
                m(n(ep1, NON_EMPTY, x, p1, y), best.get(1)),
                n(ep1, NON_EMPTY, y, p1, Bob)
        )));
        GreedyJoinOrderPlanner.Data data = createData(graph);
        Op root = GreedyJoinOrderPlanner.takeInitialJoin(data, joinCardinalityEstimator);
        assertEquals(data.pending, singleton(graph.get(2)));
        assertEquals(new HashSet<>(root.getChildren()), new HashSet<>(best));
    }

    public static class Scenario1 {
        static final StdLit name = StdLit.fromUnescaped("name");
        static final StdLit date1 = StdLit.fromUnescaped("date1");
        static final StdLit date2 = StdLit.fromUnescaped("date2");
        static final EndpointQueryOp organizationByName = n(wep, upperBound(1),
                z, p1, name,
                z, p1, t);
        static final EndpointQueryOp contracts = n(wep, guess(2), singletonList(t),
                x, p1, date1,
                x, p1, date2,
                x, p1, t,
                x, p1, b,
                x, p1, u,
                x, p1, k);
        static final EndpointQueryOp contractById = n(wep, upperBound(1), singletonList(b),
                w, p1, b,
                w, p1, d);
        static final EndpointQueryOp modalities = n(ep1, lowerBound(10),
                m, p1, c,
                m, p1, d);
        static final EndpointQueryOp procurementByUMN = n(wep, upperBound(1), asList(c, u, k),
                y, p1, c,
                y, p1, u,
                y, p1, k,
                y, p1, e);
        static final ImmutableList<Op> nodes = ImmutableList.of(organizationByName, contracts, contractById,
                modalities, procurementByUMN);
    }

    @Test(dataProvider = "joinCardinalityEstimatorData")
    public void testTakeInitialJoin(@Nonnull JoinCardinalityEstimator joinCardinalityEstimator) {
        HashSet<EndpointQueryOp> expected = Sets.newHashSet(Scenario1.organizationByName,
                                                      Scenario1.contracts);
        int i = 0;
        //noinspection UnstableApiUsage
        for (List<Op> permutation : Collections2.permutations(Scenario1.nodes)) {
            JoinGraph graph = new JoinGraph(IndexedSet.from(permutation));
            GreedyJoinOrderPlanner.Data data = createData(graph);
            Op root = GreedyJoinOrderPlanner.takeInitialJoin(data, joinCardinalityEstimator);

            assertEquals(new HashSet<>(root.getChildren()), expected, "i="+i);
            assertTrue(expected.stream().noneMatch(data.pending::contains), "i="+i);
        }
    }

    @Test(dataProvider = "joinCardinalityEstimatorData")
    public void testPlanForScenario(@Nonnull JoinCardinalityEstimator joinCardinalityEstimator) {
        GreedyJoinOrderPlanner planner
                = new GreedyJoinOrderPlanner(NoOpPerformanceListener.INSTANCE,
                                             NoCardinalityEnsemble.INSTANCE,
                                             RelativeCardinalityAdder.DEFAULT, joinCardinalityEstimator);
        JoinGraph graph = new JoinGraph(IndexedSet.from(Scenario1.nodes));
        Op root = planner.plan(graph, Scenario1.nodes);

        assertEquals(streamPreOrder(root).filter(n -> !(n instanceof JoinOp)).collect(toSet()),
                     new HashSet<>(Scenario1.nodes));
        assertEquals(streamPreOrder(root).filter(n -> !(n instanceof JoinOp)).count(),
                     Scenario1.nodes.size(), "There are duplicate leaves in the plan");

        assertTrue(root instanceof JoinOp);
        JoinOp j1 = (JoinOp)((JoinOp) root).getLeft();
        JoinOp j2 = (JoinOp) j1.getLeft();
        JoinOp j3 = (JoinOp) j2.getLeft();

        assertEquals(new HashSet<>(j3.getChildren()),
                     Sets.newHashSet(Scenario1.organizationByName, Scenario1.contracts));
        assertSame(j2.getRight(), Scenario1.contractById);

        assertSame(((JoinOp) root).getRight(), Scenario1.procurementByUMN);
        assertSame(j1.getRight(), Scenario1.modalities);
    }

}