package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.EmptyWebApiEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.Cardinality.*;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.*;

public class GreedyJoinOrderPlannerTest {
    private static final URI Alice = new StdURI("https://example.org/Alice");
    private static final URI Bob = new StdURI("https://example.org/Alice");
    private static final URI p1 = new StdURI("https://example.org/p1");
    private static final Var x = new StdVar("x");
    private static final Var y = new StdVar("y");
    private static final Var z = new StdVar("z");
    private static final Var w = new StdVar("w");
    private static final Var u = new StdVar("u");
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

    private static @Nonnull QueryNode n(@Nonnull CQEndpoint ep, Cardinality cardinality,
                                        List<Var> inputs, Term... terms) {
        CQuery.Builder b = CQuery.builder();
        for (int i = 0; i < terms.length; i +=3)
            b.add(new Triple(terms[i], terms[i+1], terms[i+2]));
        int i = 0;
        for (Var input : inputs)
            b.annotate(input, AtomAnnotation.asRequired(new Atom("Atom-" + i)));
        return new QueryNode(ep, b.build(), cardinality);
    }
    private static @Nonnull QueryNode n(@Nonnull CQEndpoint ep, Cardinality cardinality,
                                        Term... terms) {
        return n(ep, cardinality, emptyList(), terms);
    }
    private static @Nonnull MultiQueryNode m(QueryNode... nodes) {
        MultiQueryNode.Builder b = MultiQueryNode.builder();
        for (QueryNode node : nodes) b.add(node);
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
                       exact(800), 0, false, -1),
                asList(Cardinality.UNSUPPORTED, 0, false,
                       Cardinality.guess(800), 0, false, -1),

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
                                        int expected) {
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

    @Test
    public void testTakeInitialJoinSingletonGraph() {
        JoinGraph graph = new JoinGraph(IndexedSet.from(singletonList(
                n(ep1, NON_EMPTY, Alice, p1, x))
        ));
        IndexedSubset<PlanNode> pending = graph.getNodes().fullSubset();
        PlanNode node = GreedyJoinOrderPlanner.takeInitialJoin(graph, pending);
        assertSame(node, graph.get(0));
        assertTrue(pending.isEmpty());
    }

    @Test
    public void testTakeInitialJoinSingletonGraphCleansEquivalents() {
        JoinGraph graph = new JoinGraph(IndexedSet.from(singletonList(
                m(n(ep1, NON_EMPTY, Alice, p1, x), n(ep2, NON_EMPTY, Alice, p1, x))
        )));
        IndexedSubset<PlanNode> pending = graph.getNodes().fullSubset();
        PlanNode root = GreedyJoinOrderPlanner.takeInitialJoin(graph, pending);
        assertTrue(pending.isEmpty());

        assertNotSame(root, graph.get(0));
        assertTrue(graph.get(0).getChildren().contains(root));
    }


    @Test
    public void testTakeInitialJoinCleansEquivalents() {
        List<QueryNode> best = asList(
                n(ep2, guess(16), Alice, p1, x),
                n(ep2, exact(3), x, p1, y)
        );
        JoinGraph graph = new JoinGraph(IndexedSet.from(asList(
                m(n(ep1, NON_EMPTY, Alice, p1, x), best.get(0)),
                m(n(ep1, NON_EMPTY, x, p1, y), best.get(1)),
                n(ep1, NON_EMPTY, y, p1, Bob)
        )));
        IndexedSubset<PlanNode> pending = graph.getNodes().fullSubset();
        PlanNode root = GreedyJoinOrderPlanner.takeInitialJoin(graph, pending);
        assertEquals(pending, singleton(graph.get(2)));
        assertEquals(new HashSet<>(root.getChildren()), new HashSet<>(best));
    }


    @Test
    public void testTakeInitialJoin() {
        StdLit name = StdLit.fromUnescaped("name");
        StdLit date1 = StdLit.fromUnescaped("date1");
        StdLit date2 = StdLit.fromUnescaped("date2");
        QueryNode organizationByName = n(wep, upperBound(1),
                z, p1, name,
                z, p1, t);
        QueryNode contracts = n(wep, lowerBound(2), singletonList(t),
                x, p1, date1,
                x, p1, date2,
                x, p1, t,
                x, p1, b,
                x, p1, u,
                x, p1, k);
        QueryNode contractById = n(wep, upperBound(1), singletonList(b),
                w, p1, b,
                w, p1, d);
        QueryNode modalities = n(ep1, NON_EMPTY,
                m, p1, c,
                m, p1, d);
        QueryNode procurementByUMN = n(wep, upperBound(1), asList(c, u, k),
                y, p1, c,
                y, p1, u,
                y, p1, k,
                y, p1, e);
        List<PlanNode> nodes = asList(organizationByName, contracts, contractById,
                                      modalities, procurementByUMN);
        HashSet<QueryNode> expected = Sets.newHashSet(organizationByName, contracts);
        int i = 0;
        //noinspection UnstableApiUsage
        for (List<PlanNode> permutation : Collections2.permutations(nodes)) {
            JoinGraph graph = new JoinGraph(IndexedSet.from(permutation));
            IndexedSubset<PlanNode> pending = graph.getNodes().fullSubset();
            PlanNode root = GreedyJoinOrderPlanner.takeInitialJoin(graph, pending);

            assertEquals(new HashSet<>(root.getChildren()), expected, "i="+i);
            assertTrue(expected.stream().noneMatch(pending::contains), "i="+i);
        }
    }

}