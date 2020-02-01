package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinPath;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getMultiJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getPlainJoinability;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

@SuppressWarnings("UnstableApiUsage")
public class JoinPathsPlannerTest {
    private static final URI Alice = new StdURI("http://example.org/Alice");
    private static final URI Bob = new StdURI("http://example.org/Bob");
    private static final URI p1 = new StdURI("http://example.org/p1");
    private static final URI p2 = new StdURI("http://example.org/p2");
    private static final URI p3 = new StdURI("http://example.org/p3");
    private static final URI p4 = new StdURI("http://example.org/p4");
    private static final Var x = new StdVar("x");
    private static final Var y = new StdVar("y");
    private static final Var z = new StdVar("z");

    private static final Atom Person = new Atom("Person"), Atom1 = new Atom("Atom1");

    private static final EmptyEndpoint e1 = new EmptyEndpoint(),
                                       e2 = new EmptyEndpoint(), e3 = new EmptyEndpoint();

    private static  @Nonnull QueryNode node(CQEndpoint ep, @Nonnull Term... terms) {
        CQuery.Builder builder = CQuery.builder();
        for (int i = 0; i < terms.length; i += 3)
            builder.add(new Triple(terms[i], terms[i+1], terms[i+2]));
        return new QueryNode(ep, builder.build());
    }
    private static @Nonnull MultiQueryNode m(@Nonnull QueryNode... nodes) {
        Preconditions.checkArgument(nodes.length > 1);
        Preconditions.checkArgument(Arrays.stream(nodes).allMatch(Objects::nonNull));
        return MultiQueryNode.builder().addAll(stream(nodes).collect(toList())).build();
    }

    private boolean isBroken(@Nonnull List<JoinInfo> path) {
        JoinInfo last = null;
        for (JoinInfo info : path) {
            if (last != null && !info.isLinkedTo(last))
                return true;
            last = info;
        }
        return false;
    }

    @DataProvider
    public static Object[][] pathEqualsData() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p1, y);
        QueryNode n3 = node(e2, y, p1, Bob);
        return Stream.of(
                asList(new JoinPath(n1), new JoinPath(n1), true),
                asList(new JoinPath(n1), new JoinPath(n2), false),
                asList(new JoinPath(getPlainJoinability(n1, n2)),
                       new JoinPath(getPlainJoinability(n1, n2)), true),
                asList(new JoinPath(getPlainJoinability(n2, n1)),
                       new JoinPath(getPlainJoinability(n1, n2)), true),
                asList(new JoinPath(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3)),
                       new JoinPath(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3)), true),
                asList(new JoinPath(getPlainJoinability(n3, n2), getPlainJoinability(n2, n1)),
                       new JoinPath(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3)), true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "pathEqualsData")
    public void testPathEquals(@Nonnull JoinPath a, @Nonnull JoinPath b, boolean expected) {
        if (expected) {
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
        } else {
            assertNotEquals(a, b);
        }
    }

    @Test
    public void testState() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p2, y), n2Alt = node(e2, x, p2, y);
        QueryNode n3 = node(e1, y, p3, z);
        QueryNode n4 = new QueryNode(e2, CQuery.with(new Triple(z, p4, Bob))
                .annotate(z, AtomAnnotation.asRequired(Atom1))
                .annotate(Bob, AtomAnnotation.of(Person)).build());

        IndexedSet<Triple> allTriples = IndexedSet.fromDistinct(
                asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
                       new Triple(y, p3, z), new Triple(z, p4, Bob)));

        JoinPathsPlanner.State s0 = JoinPathsPlanner.State.start(allTriples, n1);
        assertEquals(s0.toPath(), new JoinPath(n1));
        assertEquals(s0.depth, 0);
        assertFalse(s0.hasInputs());

        JoinPathsPlanner.State s1 = s0.advance(getPlainJoinability(n1, n2), n2);
        assertNotNull(s1);
        assertEquals(s1.toPath(), new JoinPath(getPlainJoinability(n1, n2)));
        assertEquals(s1.depth, 1);
        assertFalse(s0.hasInputs());

        assertNull(s1.advance(getPlainJoinability(n2, n2Alt), n2Alt));

        JoinPathsPlanner.State s2 = s1.advance(getPlainJoinability(n2, n3), n3);
        assertNotNull(s2);
        assertEquals(s2.toPath(), new JoinPath(getPlainJoinability(n2, n3),
                                               getPlainJoinability(n1, n2)));
        assertEquals(s2.depth, 2);
        assertFalse(s0.hasInputs());

        assertNull(s2.advance(getPlainJoinability(n3, n2Alt), n2Alt));

        JoinPathsPlanner.State s3 = s2.advance(getPlainJoinability(n3, n4), n4);
        assertNotNull(s3);
        assertEquals(s3.toPath(), new JoinPath(getPlainJoinability(n3, n4),
                                               getPlainJoinability(n2, n3),
                                               getPlainJoinability(n1, n2)));
        assertEquals(s3.depth, 3);
        assertFalse(s0.hasInputs());
    }

    @Test
    public void testBuildPath() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p2, y);
        QueryNode n3 = new QueryNode(e2, CQuery.with(new Triple(y, p3, Bob))
                .annotate(y, AtomAnnotation.asRequired(Atom1))
                .annotate(Bob, AtomAnnotation.of(Person)).build());

        JoinPath path1 = new JoinPath(getPlainJoinability(n3, n2), getPlainJoinability(n2, n1));
        JoinPath path2 = new JoinPath(getPlainJoinability(n2, n1), getPlainJoinability(n3, n2));
        JoinPath path3 = new JoinPath(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3));

        assertEquals(path1.getNodes(), Sets.newHashSet(n1, n2, n3));
        assertEquals(path2.getNodes(), Sets.newHashSet(n1, n2, n3));
        assertEquals(path3.getNodes(), Sets.newHashSet(n1, n2, n3));

        assertEquals(path1, path2);
        assertEquals(path1, path3);
        assertEquals(path2, path3);
    }

    @Test
    public void testStateInputs() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p2, y);
        QueryNode n3 = new QueryNode(e2, CQuery.with(new Triple(y, p3, Bob))
                .annotate(y, AtomAnnotation.asRequired(Atom1))
                .annotate(Bob, AtomAnnotation.of(Person)).build());

        IndexedSet<Triple> allTriples = IndexedSet.fromDistinct(
                asList(new Triple(Alice, p1, x), new Triple(x, p2, y), new Triple(y, p3, Bob)));

        JoinPathsPlanner.State s0 = JoinPathsPlanner.State.start(allTriples, n3);
        assertEquals(s0.depth, 0);
        assertTrue(s0.hasInputs());
        assertEquals(s0.toPath(), new JoinPath(n3));

        JoinPathsPlanner.State s1 = s0.advance(getPlainJoinability(n3, n2), n2);
        assertNotNull(s1);
        assertEquals(s1.depth, 1);
        assertFalse(s1.hasInputs());
        assertEquals(s1.toPath(), new JoinPath(getPlainJoinability(n3, n2)));

        JoinPathsPlanner.State s2 = s1.advance(getPlainJoinability(n2, n1), n1);
        assertNotNull(s2);
        assertEquals(s2.depth, 2);
        assertFalse(s2.hasInputs());
        assertEquals(s2.toPath(), new JoinPath(getPlainJoinability(n3, n2),
                                               getPlainJoinability(n2, n1)));

    }

    private static boolean nodeMatch(@Nonnull PlanNode actual, @Nonnull PlanNode expected) {
        if (expected instanceof MultiQueryNode) {
            if (!(actual instanceof MultiQueryNode)) return false;
            if (expected.getChildren().size() != actual.getChildren().size()) return false;
            return expected.getChildren().stream()
                    .anyMatch(e -> actual.getChildren().stream().anyMatch(a -> nodeMatch(a, e)));
        }
        return actual.equals(expected);
    }

    @DataProvider
    public static Object[][] groupNodesData() {
        QueryNode n1 = node(e1, Alice, p1, x), n2 = node(e1, x, p2, y), n3 = node(e1, y, p3, Bob);
        QueryNode o1 = node(e2, Alice, p1, x), o2 = node(e2, x, p2, y), o3 = node(e2, y, p3, Bob);
        QueryNode i2 = new QueryNode(e2, CQuery.with(new Triple(x, p2, y))
                .annotate(x, AtomAnnotation.asRequired(Atom1))
                .annotate(y, AtomAnnotation.of(Atom1)).build());

        return Stream.of(
                asList(singleton(n1), singleton(n1)),
                asList(asList(n1, n2, n3), asList(n1, n2, n3)),
                asList(asList(n1, n2, o1), asList(m(n1, o1), n2)),
                asList(asList(n2, i2), asList(n2, i2)),
                asList(asList(n1, n2, i2), asList(n1, n2, i2)),
                asList(asList(n2, o2, i2), asList(m(n2, o2), i2)),
                asList(asList(n2, o2, i2, n3), asList(m(n2, o2), i2, n3)),
                asList(asList(n1, o1, n2, o2, i2, n3), asList(m(n1, o1), m(n2, o2), i2, n3)),
                asList(asList(n1, o1, n2, o2, i2, n3, o3), asList(m(n1,o1), m(n2,o2), i2, m(n3,o3)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "groupNodesData")
    public void testGroupNodes(Collection<QueryNode> in, Collection<PlanNode> expected) {
        for (List<QueryNode> permutation : permutations(in)) {
            JoinPathsPlanner planner = new JoinPathsPlanner(new ArbitraryJoinOrderPlanner());
            List<PlanNode> grouped = planner.groupNodes(permutation);
            assertEquals(grouped.size(), expected.size());
            for (PlanNode expectedNode : expected) {
                assertTrue(grouped.stream().anyMatch(actual -> nodeMatch(actual, expectedNode)),
                        "No match for " + expectedNode);
            }
        }
    }

    @DataProvider
    public static Object[][] pathsData() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p1, y);
        QueryNode n3 = node(e1, Alice, p1, x, x, p1, y);
        QueryNode n4 = node(e1, y, p1, Bob);
        QueryNode n5 = node(e1, y, p2, Bob);
        QueryNode n6 = node(e1, y, p2, x);

        // n*i :: SUBJ is input
        QueryNode n1i = new QueryNode(e2, CQuery.with(new Triple(Alice, p1, x))
                .annotate(Alice, AtomAnnotation.asRequired(Person))
                .annotate(x, AtomAnnotation.of(Atom1)).build());
        QueryNode n2i = new QueryNode(e2, CQuery.with(new Triple(x, p1, y))
                .annotate(x, AtomAnnotation.asRequired(Atom1))
                .annotate(y, AtomAnnotation.of(Atom1)).build());
        QueryNode n4i = new QueryNode(e2, CQuery.with(new Triple(y, p1, Bob))
                .annotate(y, AtomAnnotation.asRequired(Atom1))
                .annotate(Bob, AtomAnnotation.of(Person)).build());
        QueryNode n5i = new QueryNode(e2, CQuery.with(new Triple(y, p2, Bob))
                .annotate(y, AtomAnnotation.asRequired(Atom1))
                .annotate(Bob, AtomAnnotation.of(Person)).build());

        // n*j :: OBJ is input
        QueryNode n1j = new QueryNode(e3, CQuery.with(new Triple(Alice, p1, x))
                .annotate(Alice, AtomAnnotation.of(Person))
                .annotate(x, AtomAnnotation.asRequired(Atom1)).build());
        QueryNode n2j = new QueryNode(e3, CQuery.with(new Triple(x, p1, y))
                .annotate(x, AtomAnnotation.of(Atom1))
                .annotate(y, AtomAnnotation.asRequired(Atom1)).build());
        QueryNode n5j = new QueryNode(e3, CQuery.with(new Triple(y, p2, Bob))
                .annotate(y, AtomAnnotation.of(Atom1))
                .annotate(Bob, AtomAnnotation.asRequired(Person)).build());

        // mXi == M(nX, nXi)
        MultiQueryNode m1i = m(n1, n1i);

        return Stream.of(
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y)),
                        asList(n1, n2),
                        singleton(new JoinPath(getPlainJoinability(n1, n2)))),
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y)),
                        singletonList(n3),
                        singleton(new JoinPath(n3))),
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y)),
                        asList(n1, n2, n3),
                        asList(new JoinPath(getPlainJoinability(n1, n2)), new JoinPath(n3))),

                // n1 -> n2 --> n4
                //          +-> n4i
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y),
                                   new Triple(y, p1, Bob)),
                        asList(n1, n2, n4, n4i),
                        asList(new JoinPath(getPlainJoinability(n1, n2),
                                            getPlainJoinability(n2, n4)),
                               new JoinPath(getPlainJoinability(n1, n2),
                                            getPlainJoinability(n2, n4i)))),

                // m1i +-> n2  --+         +--> n5
                //     |         +--> n4 --+
                //     +-> n2i --+         +--> n5i
                //
                asList(CQuery.from(new Triple(Alice, p1, x  ), new Triple(x, p1, y  ),
                                   new Triple(y,     p1, Bob), new Triple(y, p2, Bob)),
                        asList(m1i, n2, n4, n5, n2i, n5i),
                        asList(new JoinPath(getMultiJoinability(m1i, n2),
                                            getMultiJoinability(n2, n4),
                                            getMultiJoinability(n4, n5)),
                               new JoinPath(getMultiJoinability(m1i, n2),
                                            getMultiJoinability(n2, n4),
                                            getMultiJoinability(n4, n5i)),
                               new JoinPath(getMultiJoinability(m1i, n2i),
                                            getMultiJoinability(n2i, n4),
                                            getMultiJoinability(n4, n5)),
                               new JoinPath(getMultiJoinability(m1i, n2i),
                                            getMultiJoinability(n2i, n4),
                                            getMultiJoinability(n4, n5i))
                        )),

                // n1j <- n2j <--+          +--  n5j
                //         |     |          |
                //   +-----+     +--> n4 <--+
                //   v           |          |
                // n1i -> n2i  --+          +--> n5i
                asList(CQuery.from(new Triple(Alice, p1, x  ), new Triple(x, p1, y  ),
                                   new Triple(y,     p1, Bob), new Triple(y, p2, Bob)),
                        asList(n1j, n2j, n4, n5j, n1i, n2i, n5i),
                        asList(new JoinPath(getPlainJoinability(n1i, n2i),
                                            getPlainJoinability(n2i, n4),
                                            getPlainJoinability(n4, n5i)),
                               new JoinPath(getPlainJoinability(n1i, n2i),
                                            getPlainJoinability(n2i, n4),
                                            getPlainJoinability(n5j, n4)),

                               new JoinPath(getPlainJoinability(n5j, n4),
                                            getPlainJoinability(n4, n2j),
                                            getPlainJoinability(n2j, n1j)),
                               new JoinPath(getPlainJoinability(n5j, n4),
                                            getPlainJoinability(n4, n2j),
                                            getPlainJoinability(n2j, n1i)),

                               new JoinPath(getPlainJoinability(n4, n5i),
                                            getPlainJoinability(n4, n2j),
                                            getPlainJoinability(n2j, n1j)),
                               new JoinPath(getPlainJoinability(n4, n5i),
                                            getPlainJoinability(n4, n2j),
                                            getPlainJoinability(n2j, n1i))
                        )),

                //  +------+------+
                //  |      |      |
                //  v      v      v
                // n1 <-> n2 <-> n6
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y),
                                   new Triple(y, p2, x)),
                       asList(n1, n2, n6),
                       singletonList(new JoinPath(getPlainJoinability(n1, n2),
                                                  getPlainJoinability(n2, n6)))),

                //  +------+------+
                //  |      |      |
                //  v      v      |
                // n1j <- n2j <- n6
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y),
                        new Triple(y, p2, x)),
                        asList(n1j, n2j, n6),
                        singletonList(new JoinPath(getPlainJoinability(n1j, n2j),
                                                   getPlainJoinability(n2j, n6)))),

                //         +-------+
                //  +------|------+|
                //  |      |      ||
                //  v      v      v|
                // n1i -> n2i -> n6i
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y),
                        new Triple(y, p2, x)),
                        asList(n1i, n2i, n6),
                        singletonList(new JoinPath(getPlainJoinability(n1i, n2i),
                                                   getPlainJoinability(n2i, n6)))),

                //          +----n5j
                //          v
                //  n1j <- n2 -> n5i
                asList(CQuery.from(new Triple(Alice, p1, x), new Triple(x, p1, y),
                                   new Triple(y, p2, Bob)),
                       asList(n1j, n2, n5i, n5j),
                       asList(new JoinPath(getPlainJoinability(n2, n1j),
                                           getPlainJoinability(n2, n5i)),
                              new JoinPath(getPlainJoinability(n5j, n2),
                                           getPlainJoinability(n2, n1j))))
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "pathsData")
    public void testPaths(CQuery query, List<PlanNode> nodes, Collection<JoinPath> expectedPaths) {
        for (List<PlanNode> permutation : permutations(nodes)) {
            Set<JoinPath> paths = new HashSet<>();
            JoinGraph g = new JoinGraph(permutation);
            JoinPathsPlanner planner = new JoinPathsPlanner(new ArbitraryJoinOrderPlanner());
            planner.getPaths(IndexedSet.fromDistinctCopy(query.getMatchedTriples()), g, paths);

            List<JoinPath> e = emptyList();
            assertEquals(paths.stream().filter(JoinPath::hasJoins)
                    .map(JoinPath::getJoinInfos)
                    .filter(this::isBroken).collect(toList()), e);

            HashSet<JoinPath> exSet = new HashSet<>(expectedPaths);
            assertEquals(paths.stream().filter(p -> !exSet.contains(p)).collect(toList()), e);
            assertEquals(exSet.stream().filter(p -> !paths.contains(p)).collect(toList()), e);
            assertEquals(paths, exSet);
        }
    }

}