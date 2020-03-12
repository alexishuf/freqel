package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.UndirectedIrreflexiveArrayGraph;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getMultiJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getPlainJoinability;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.*;

public class JoinInfoTest implements TestContext {
    private static final Atom X = new Atom("X");
    private static final Atom Y = new Atom("Y");
    private static final Atom Z = new Atom("Z");


    private static final CQuery xpy = CQuery.from(new Triple(x, p1, y));
    private static final CQuery ypz = CQuery.from(new Triple(y, p1, z));
    private static final CQuery zpw = CQuery.from(new Triple(z, p1, w));
    private static final CQuery zpx = CQuery.from(new Triple(z, p1, x));

    private static final CQuery xpyi = CQuery.with(new Triple(x, p1, y))
            .annotate(x, AtomAnnotation.of(X))
            .annotate(y, asRequired(Y, "Y")).build();
    private static final CQuery yipz = CQuery.with(new Triple(y, p1, z))
            .annotate(y, asRequired(Y, "Y"))
            .annotate(z, AtomAnnotation.of(Z)).build();


    private static @Nonnull PlanNode node(@Nonnull CQuery... queries) {
        return node(1, queries);
    }

    private static @Nonnull PlanNode node(int endpoints, @Nonnull CQuery... queries) {
        Preconditions.checkArgument(queries.length > 0);
        Preconditions.checkArgument(endpoints > 0);
        List<EmptyEndpoint> endpointList = new ArrayList<>(endpoints);
        for (int i = 0; i < endpoints; i++) {
            endpointList.add(new EmptyEndpoint());
        }
        if (queries.length == 1 && endpoints == 1)
            return new QueryNode(endpointList.get(0), queries[0]);
        MultiQueryNode.Builder builder = MultiQueryNode.builder();
        for (int i = 0; i < endpoints; i++) {
            for (CQuery query : queries)
                builder.add(new QueryNode(endpointList.get(i), query));
        }
        return builder.build();
    }


    @DataProvider
    public static Object[][] plainData() {
        List<List<Object>> data = asList(
                asList(node(xpy), node(ypz), singleton("y"), emptySet(), false),
                asList(node(xpy), node(zpw), emptySet(), emptySet(), false),
                asList(node(xpy), node(xpy), emptySet(), emptySet(), true),
                asList(node(xpyi), node(ypz), singleton("y"), emptySet(), false),
                asList(node(xpyi), node(zpx), singleton("x"), singleton("y"), false),

                //ignore MultiQueryNodes
                asList(node(3, xpy), node(3, ypz), singleton("y"), emptySet(), false),
                asList(node(3, xpy), node(3, zpw), emptySet(), emptySet(), false),
                asList(node(3, xpy), node(3, xpy), emptySet(), emptySet(), true),
                asList(node(3, xpyi), node(3, ypz), singleton("y"), emptySet(), false),
                asList(node(3, xpyi), node(3, zpx), singleton("x"), singleton("y"), false),

                asList(JoinNode.builder(node(xpy), node(ypz)).build(), node(zpw),
                        singleton("z"), emptySet(), false),
                asList(JoinNode.builder(node(xpy), node(ypz)).build(), node(xpy),
                        emptySet(), emptySet(), true),
                asList(JoinNode.builder(node(xpy), node(ypz)).build(), node(ypz),
                        emptySet(), emptySet(), true),
                asList(JoinNode.builder(node(xpy), node(zpx)).build(), node(ypz),
                        asList("y", "z"), emptySet(), false),

                //ignore MultiQueryNodes at leafs
                asList(JoinNode.builder(node(3, xpy), node(3, ypz)).build(), node(3, zpw),
                        singleton("z"), emptySet(), false),
                asList(JoinNode.builder(node(3, xpy), node(3, ypz)).build(), node(3, xpy),
                        emptySet(), emptySet(), true),

                //ignore MultiQueryNodes that have bad child joins
                asList(node(ypz, yipz), node(xpy, xpyi), singleton("y"), emptySet(), false),
                asList(node(ypz, yipz), node(xpyi), singleton("y"), emptySet(), false)
        );
        return data.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "plainData")
    public void testPlain(@Nonnull PlanNode l, @Nonnull PlanNode r,
                          @Nonnull Collection<String> joinVars,
                          @Nonnull Collection<String> pendingInputs, boolean subsumed) {
        for (JoinInfo j : asList(getPlainJoinability(l, r), getPlainJoinability(r, l))) {
            assertEquals(j.isValid(), !joinVars.isEmpty());
            assertEquals(j.getJoinVars(), new HashSet<>(joinVars));
            assertEquals(j.getPendingInputs(), new HashSet<>(pendingInputs));
            assertEquals(j.isSubsumed(), subsumed);

            if (j.getLeftNodes().equals(singletonList(l))) {
                assertEquals(j.getRightNodes(), singletonList(r));
            } else {
                assertEquals(j.getLeftNodes(), singletonList(r));
                assertEquals(j.getRightNodes(), singletonList(l));
            }
        }
    }

    @Test(dataProvider = "plainData")
    public void testPlainReflexive(@Nonnull PlanNode l, @Nonnull PlanNode r,
                                   @Nonnull Collection<String> joinVars,
                                   @Nonnull Collection<String> pendingInputs, boolean subsumed) {
        JoinInfo from = getPlainJoinability(l, r);
        JoinInfo to = getPlainJoinability(r, l);
        assertEquals(from, from);
        assertEquals(to, to);
        assertEquals(from.isValid(), to.isValid());
        assertEquals(from.getJoinVars(), to.getJoinVars());
        assertEquals(from.getPendingInputs(), to.getPendingInputs());
        assertEquals(from.isSubsumed(), to.isSubsumed());
        assertEquals(from.getChildJoins(), to.getChildJoins());
        assertEquals(from.getLeftNodes(), to.getRightNodes());
        assertEquals(from.getRightNodes(), to.getLeftNodes());
        assertEquals(from, to);
    }

    @DataProvider
    public static Object[][] multiData() {
        PlanNode nxpy = node(xpy);
        PlanNode nypz = node(ypz);
        PlanNode nyipz = node(yipz);
        PlanNode nxpyi = node(xpyi);
        PlanNode mxpyi = MultiQueryNode.builder().add(nxpy).add(nxpyi).build();
        PlanNode myipz = MultiQueryNode.builder().add(nypz).add(nyipz).build();

        return Stream.of(
                asList(nxpy, nypz, singletonList("y"), emptySet(), false,
                        singletonList(ImmutablePair.of(nxpy, nypz))),
                asList(nxpy, node(xpy), emptySet(), emptySet(), true, emptyList()),
                asList(nxpy, node(xpyi), emptySet(), emptySet(), true, emptyList()),
                asList(nxpy, node(xpy, xpyi), emptySet(), emptySet(), true, emptyList()),
                asList(mxpyi, nypz, singletonList("y"), emptySet(), false,
                       asList(ImmutablePair.of(nxpy, nypz), ImmutablePair.of(nxpyi, nypz))),
                asList(mxpyi, nyipz, singletonList("y"), emptySet(), false,
                       singletonList(ImmutablePair.of(nxpy, nyipz))),
                asList(mxpyi, myipz, singletonList("y"), emptySet(), false,
                       asList(ImmutablePair.of(nxpy, nypz), ImmutablePair.of(nxpy, nyipz),
                              ImmutablePair.of(nxpyi, nypz)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "multiData")
    public void testMultiJoinability(@Nonnull PlanNode l, @Nonnull PlanNode r,
                                     @Nonnull Collection<String> joinVars,
                                     @Nonnull Collection<String> pendingInputs, boolean subsumed,
                                     @Nonnull List<ImmutablePair<PlanNode, PlanNode>> pairs) {
        for (JoinInfo j : asList(getMultiJoinability(l, r), getMultiJoinability(r, l))) {
            assertEquals(j.isValid(), !joinVars.isEmpty());
            assertEquals(j.getJoinVars(), new HashSet<>(joinVars));
            assertEquals(j.getPendingInputs(), new HashSet<>(pendingInputs));
            assertEquals(j.isSubsumed(), subsumed);

            List<PlanNode> lns = l instanceof MultiQueryNode ? l.getChildren() : singletonList(l);
            List<PlanNode> rns = r instanceof MultiQueryNode ? r.getChildren() : singletonList(r);
            if (j.getLeft() == l) {
                assertEquals(j.getLeftNodes(), lns);
                assertEquals(j.getRightNodes(), rns);
            } else {
                assertEquals(j.getLeftNodes(), rns);
                assertEquals(j.getRightNodes(), lns);

                //invert pairs, since we are on the second iteration
                List<ImmutablePair<PlanNode, PlanNode>> tmp = new ArrayList<>();
                for (ImmutablePair<PlanNode, PlanNode> pair : pairs)
                    tmp.add(ImmutablePair.of(pair.right, pair.left));
                pairs = tmp;
            }

            assertEquals(j.getChildJoins().keySet(), new HashSet<>(pairs));
            for (ImmutablePair<PlanNode, PlanNode> pair : pairs) {
                JoinInfo j2 = j.getChildJoins().get(pair);
                assertTrue(j2.isValid());
                assertEquals(j2, getPlainJoinability(pair.left, pair.right));
            }
        }
    }

    @Test
    public void testJoinInfoGraph() {
        List<PlanNode> nodes =
                //     0          1          2          3          4           5
                asList(node(xpy), node(ypz), node(zpw), node(zpx), node(xpyi), node(yipz));
        UndirectedIrreflexiveArrayGraph<PlanNode, JoinInfo> g;
        g = new UndirectedIrreflexiveArrayGraph<PlanNode, JoinInfo>(JoinInfo.class, nodes) {
            @Override
            protected @Nullable JoinInfo weigh(@Nonnull PlanNode l, @Nonnull PlanNode r) {
                JoinInfo info = getPlainJoinability(l, r);
                return info.isValid() ? info : null;
            }
        };
        int[][] exValid = {{0,1}, {0,3}, {0,5}, {1, 2}, {1,3}, {1,4}, {2, 3}, {2,5}, {3,4}, {3,5}};
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i+1; j < nodes.size(); j++) {
                boolean expected = false;
                for (int[] pair : exValid)
                    expected |= pair[0] == i && pair[1] == j;
                assertEquals(g.getWeight(i, j) != null, expected);
            }
        }
        assertEquals(g.getWeight(0, 5).getJoinVars(), singleton("y"));
        assertEquals(g.getWeight(0, 5).getPendingInputs(), emptySet());
        assertEquals(g.getWeight(3, 4).getJoinVars(), singleton("x"));
        assertEquals(g.getWeight(3, 4).getPendingInputs(), singleton("y"));

        List<ImmutablePair<JoinInfo, PlanNode>> actual = new ArrayList<>();
        g.forEachNeighbor(0, (w, n) -> actual.add(ImmutablePair.of(w, n)));
        assertEquals(actual, asList(
                ImmutablePair.of(g.getWeight(0, 1), nodes.get(1)),
                ImmutablePair.of(g.getWeight(0, 3), nodes.get(3)),
                ImmutablePair.of(g.getWeight(0, 5), nodes.get(5))
        ));

        actual.clear();
        g.forEachNeighbor(nodes.get(3), (w, n) -> actual.add(ImmutablePair.of(w, n)));
        assertEquals(actual, asList(
                ImmutablePair.of(g.getWeight(0, 3), nodes.get(0)),
                ImmutablePair.of(g.getWeight(1, 3), nodes.get(1)),
                ImmutablePair.of(g.getWeight(2, 3), nodes.get(2)),
                ImmutablePair.of(g.getWeight(3, 4), nodes.get(4)),
                ImmutablePair.of(g.getWeight(3, 5), nodes.get(5))
        ));
    }

    @DataProvider
    public static Object[][] isLinkedToData() {
        PlanNode nxpy = node(xpy), nypz = node(ypz), nyipz = node(yipz), nzpw = node(zpw);
        MultiQueryNode mypz = MultiQueryNode.builder().add(nyipz).add(nypz).build();
        return Stream.of(
                asList(getPlainJoinability(nxpy, nypz), getPlainJoinability(nypz,  nzpw), true),
                asList(getPlainJoinability(nypz, nxpy), getPlainJoinability(nzpw,  nypz), true),
                asList(getPlainJoinability(nxpy, nypz), getPlainJoinability(nyipz, nzpw), false),

                asList(getPlainJoinability(nxpy, mypz), getPlainJoinability(mypz, nzpw), true),
                asList(getPlainJoinability(nxpy, mypz), getPlainJoinability(nypz, nzpw), false),
                asList(getPlainJoinability(mypz, nxpy), getPlainJoinability(nzpw, mypz), true),

                asList(getMultiJoinability(nxpy, mypz), getMultiJoinability(mypz, nzpw), true),
                asList(getMultiJoinability(nxpy, mypz), getMultiJoinability(nypz, nzpw), false),
                asList(getMultiJoinability(mypz, nxpy), getMultiJoinability(nzpw, mypz), true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "isLinkedToData")
    public void testIsLinkedTo(JoinInfo info, JoinInfo other, boolean expected) {
        assertEquals(info.isLinkedTo(other), expected);
        assertEquals(other.isLinkedTo(info), expected);
    }

    @Test
    public void testGetByPosition() {
        PlanNode n1 = node(xpy), n2 = node(ypz);
        JoinInfo info1 = getPlainJoinability(n1, n2);
        assertSame(info1.get(JoinInfo.Position.LEFT), n1);
        assertSame(info1.get(JoinInfo.Position.RIGHT), n2);
        assertEquals(info1.getNodes(JoinInfo.Position.LEFT), singleton(n1));
        assertEquals(info1.getNodes(JoinInfo.Position.RIGHT), singleton(n2));


        PlanNode m1 = node(xpy, xpyi), m2 = node(ypz, yipz);
        JoinInfo info2 = getMultiJoinability(m1, m2);
        assertSame(info2.get(JoinInfo.Position.LEFT), m1);
        assertSame(info2.get(JoinInfo.Position.RIGHT), m2);
        assertEquals(info2.getNodes(JoinInfo.Position.LEFT), m1.getChildren());
        assertEquals(info2.getNodes(JoinInfo.Position.RIGHT), m2.getChildren());
    }


    @DataProvider
    public static Object[][] oppositeToLinkedData() {
        PlanNode nxpy = node(xpy), nypz = node(ypz), nyipz = node(yipz), nzpw = node(zpw);
        PlanNode mxpy = node(xpy, xpyi);
        MultiQueryNode mypz = MultiQueryNode.builder().add(nyipz).add(nypz).build();
        return Stream.of(
                asList(getPlainJoinability(nxpy, nypz), getPlainJoinability(nypz, nzpw), nxpy),
                asList(getPlainJoinability(nypz, nzpw), getPlainJoinability(nxpy, nypz), nzpw),
                asList(getPlainJoinability(nypz, nxpy), getPlainJoinability(nzpw, nypz), nxpy),
                asList(getPlainJoinability(nzpw, nypz), getPlainJoinability(nypz, nxpy), nzpw),

                asList(getPlainJoinability(nypz, nxpy), getPlainJoinability(nypz, nzpw), nxpy),
                asList(getPlainJoinability(nypz, nzpw), getPlainJoinability(nypz, nxpy), nzpw),

                asList(getPlainJoinability(nxpy, nypz), getPlainJoinability(nzpw, nypz), nxpy),
                asList(getPlainJoinability(nzpw, nypz), getPlainJoinability(nxpy, nypz), nzpw),

                asList(getPlainJoinability(nxpy, mypz), getPlainJoinability(mypz, nzpw), nxpy),
                asList(getPlainJoinability(mypz, nzpw), getPlainJoinability(nxpy, mypz), nzpw),
                asList(getPlainJoinability(mypz, nxpy), getPlainJoinability(nzpw, mypz), nxpy),
                asList(getPlainJoinability(nzpw, mypz), getPlainJoinability(mypz, nxpy), nzpw),

                asList(getMultiJoinability(mxpy, nypz), getMultiJoinability(nypz, nzpw), mxpy),
                asList(getMultiJoinability(nypz, nzpw), getMultiJoinability(mxpy, nypz), nzpw),
                asList(getMultiJoinability(nypz, mxpy), getMultiJoinability(nzpw, nypz), mxpy),
                asList(getMultiJoinability(nzpw, nypz), getMultiJoinability(nypz, mxpy), nzpw)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "oppositeToLinkedData")
    public void testOppositeToLinked(JoinInfo info, JoinInfo other, PlanNode expected) {
        assertTrue(info.isLinkedTo(other));
        assertTrue(other.isLinkedTo(info));
        assertSame(info.getOppositeToLinked(other), expected);
    }
}