package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinComponent;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.util.IndexedSet.fromDistinctCopy;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@SuppressWarnings("UnstableApiUsage")
public class JoinPathsPlannerTest implements TestContext {
    private static final Atom Person = new Atom("Person"), Atom1 = new Atom("Atom1");

    private static final EmptyEndpoint e1 = new EmptyEndpoint(), e1a = new EmptyEndpoint(),
                                       e2 = new EmptyEndpoint(), e3  = new EmptyEndpoint();

    static {
        e1.addAlternative(e1a);
    }

    private static  @Nonnull QueryNode node(CQEndpoint ep, @Nonnull Consumer<CQuery.Builder> setup,
                                            @Nonnull Term... terms) {
        CQuery.Builder builder = CQuery.builder();
        for (int i = 0; i < terms.length; i += 3)
            builder.add(new Triple(terms[i], terms[i+1], terms[i+2]));
        setup.accept(builder);
        return new QueryNode(ep, builder.build());
    }
    private static  @Nonnull QueryNode node(CQEndpoint ep, @Nonnull Term... terms) {
        return node(ep, b -> {}, terms);
    }
    private static @Nonnull MultiQueryNode m(@Nonnull QueryNode... nodes) {
        Preconditions.checkArgument(nodes.length > 1);
        Preconditions.checkArgument(Arrays.stream(nodes).allMatch(Objects::nonNull));
        return MultiQueryNode.builder().addAll(stream(nodes).collect(toList())).build();
    }

    @DataProvider
    public static Object[][] pathEqualsData() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p1, y);
        QueryNode n3 = node(e2, y, p1, Bob);
        IndexedSet<PlanNode> all = IndexedSet.fromDistinct(asList(n1, n2, n3));
        return Stream.of(
                asList(new JoinComponent(all, n1), new JoinComponent(all, n1), true),
                asList(new JoinComponent(all, n1), new JoinComponent(all, n2), false),
                asList(new JoinComponent(all, n1, n2),
                       new JoinComponent(all, n1, n2), true),
                asList(new JoinComponent(all, n2, n1),
                       new JoinComponent(all, n1, n2), true),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2, n3), true),
                asList(new JoinComponent(all, n3, n2, n1),
                       new JoinComponent(all, n1, n2, n3), true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "pathEqualsData")
    public void testPathEquals(@Nonnull JoinComponent a, @Nonnull JoinComponent b, boolean expected) {
        if (expected) {
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
        } else {
            assertNotEquals(a, b);
        }
    }

//    @Test
//    public void testState() {
//        QueryNode n1 = node(e1, Alice, p1, x);
//        QueryNode n2 = node(e1, x, p2, y), n2Alt = node(e2, x, p2, y);
//        QueryNode n3 = node(e1, y, p3, z);
//        QueryNode n4 = new QueryNode(e2, CQuery.with(new Triple(z, p4, Bob))
//                .annotate(z, asRequired(Atom1, "Atom1"))
//                .annotate(Bob, AtomAnnotation.of(Person)).build());
//
//        IndexedSet<Triple> allTriples = IndexedSet.fromDistinct(
//                asList(new Triple(Alice, p1, x), new Triple(x, p2, y),
//                       new Triple(y, p3, z), new Triple(z, p4, Bob)));
//        IndexedSet<PlanNode> allNodes = IndexedSet.fromDistinct(asList(n1, n2, n3, n4));
//
//        JoinPathsPlanner.State s0 = JoinPathsPlanner.State.start(allTriples, n1);
//        assertEquals(s0.toPath(allNodes), new JoinComponent(allNodes, n1));
//        assertEquals(s0.depth, 0);
//        assertFalse(s0.hasInputs());
//
//        JoinPathsPlanner.State s1 = s0.advance(getPlainJoinability(n1, n2), n2);
//        assertNotNull(s1);
//        assertEquals(s1.toPath(allNodes), new JoinComponent(allNodes, getPlainJoinability(n1, n2)));
//        assertEquals(s1.depth, 1);
//        assertFalse(s0.hasInputs());
//
//        assertNull(s1.advance(getPlainJoinability(n2, n2Alt), n2Alt));
//
//        JoinPathsPlanner.State s2 = s1.advance(getPlainJoinability(n2, n3), n3);
//        assertNotNull(s2);
//        assertEquals(s2.toPath(allNodes), new JoinComponent(allNodes, getPlainJoinability(n2, n3),
//                                                                 getPlainJoinability(n1, n2)));
//        assertEquals(s2.depth, 2);
//        assertFalse(s0.hasInputs());
//
//        assertNull(s2.advance(getPlainJoinability(n3, n2Alt), n2Alt));
//
//        JoinPathsPlanner.State s3 = s2.advance(getPlainJoinability(n3, n4), n4);
//        assertNotNull(s3);
//        assertEquals(s3.toPath(allNodes), new JoinComponent(allNodes, getPlainJoinability(n3, n4),
//                                                                 getPlainJoinability(n2, n3),
//                                                                 getPlainJoinability(n1, n2)));
//        assertEquals(s3.depth, 3);
//        assertFalse(s0.hasInputs());
//    }

    @Test
    public void testBuildPath() {
        QueryNode n1 = node(e1, Alice, p1, x);
        QueryNode n2 = node(e1, x, p2, y);
        QueryNode n3 = new QueryNode(e2, CQuery.with(new Triple(y, p3, Bob))
                .annotate(y, asRequired(Atom1, "Atom1"))
                .annotate(Bob, AtomAnnotation.of(Person)).build());
        IndexedSet<PlanNode> nodes = IndexedSet.fromDistinct(asList(n1, n2, n3));

        JoinComponent path1, path2, path3;
        path1 = new JoinComponent(nodes, n3, n2, n1);
        path2 = new JoinComponent(nodes, n2, n1, n3);
        path3 = new JoinComponent(nodes, n1, n2, n3);

        assertEquals(path1.getNodes(), Sets.newHashSet(n1, n2, n3));
        assertEquals(path2.getNodes(), Sets.newHashSet(n1, n2, n3));
        assertEquals(path3.getNodes(), Sets.newHashSet(n1, n2, n3));

        assertEquals(path1, path2);
        assertEquals(path1, path3);
        assertEquals(path2, path3);
    }

//    @Test
//    public void testStateInputs() {
//        QueryNode n1 = node(e1, Alice, p1, x);
//        QueryNode n2 = node(e1, x, p2, y);
//        QueryNode n3 = new QueryNode(e2, CQuery.with(new Triple(y, p3, Bob))
//                .annotate(y, asRequired(Atom1, "Atom1"))
//                .annotate(Bob, AtomAnnotation.of(Person)).build());
//
//        IndexedSet<Triple> allTriples = IndexedSet.fromDistinct(
//                asList(new Triple(Alice, p1, x), new Triple(x, p2, y), new Triple(y, p3, Bob)));
//        IndexedSet<PlanNode> nodes = IndexedSet.fromDistinct(asList(n1, n2, n3));
//
//        JoinPathsPlanner.State s0 = JoinPathsPlanner.State.start(allTriples, n3);
//        assertEquals(s0.depth, 0);
//        assertTrue(s0.hasInputs());
//        assertEquals(s0.toPath(nodes), new JoinComponent(nodes, n3));
//
//        JoinPathsPlanner.State s1 = s0.advance(getPlainJoinability(n3, n2), n2);
//        assertNotNull(s1);
//        assertEquals(s1.depth, 1);
//        assertFalse(s1.hasInputs());
//        assertEquals(s1.toPath(nodes), new JoinComponent(nodes, getPlainJoinability(n3, n2)));
//
//        JoinPathsPlanner.State s2 = s1.advance(getPlainJoinability(n2, n1), n1);
//        assertNotNull(s2);
//        assertEquals(s2.depth, 2);
//        assertFalse(s2.hasInputs());
//        assertEquals(s2.toPath(nodes), new JoinComponent(nodes, getPlainJoinability(n3, n2),
//                                                           getPlainJoinability(n2, n1)));
//
//    }

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
                .annotate(x, asRequired(Atom1, "Atom1"))
                .annotate(y, AtomAnnotation.of(Atom1)).build());
        QueryNode aliceKnowsX = node(e1, Alice, knows, x), yKnowsBob = node(e1, y, knows, Bob);

        return Stream.of(
                asList(singleton(n1), singleton(n1)),
                asList(asList(n1, n2, n3), asList(n1, n2, n3)),
                asList(asList(n1, n2, o1), asList(m(n1, o1), n2)),
                asList(asList(n2, i2), asList(n2, i2)),
                asList(asList(n1, n2, i2), asList(n1, n2, i2)),
                asList(asList(n2, o2, i2), asList(m(n2, o2), i2)),
                asList(asList(n2, o2, i2, n3), asList(m(n2, o2), i2, n3)),
                asList(asList(n1, o1, n2, o2, i2, n3), asList(m(n1, o1), m(n2, o2), i2, n3)),
                asList(asList(n1, o1, n2, o2, i2, n3, o3),
                       asList(m(n1,o1), m(n2,o2), i2, m(n3,o3))),
                asList(asList(aliceKnowsX, yKnowsBob), asList(aliceKnowsX, yKnowsBob))
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
                .annotate(Alice, asRequired(Person, "Person"))
                .annotate(x, AtomAnnotation.of(Atom1)).build());
        QueryNode n2i = new QueryNode(e2, CQuery.with(new Triple(x, p1, y))
                .annotate(x, asRequired(Atom1, "Atom1"))
                .annotate(y, AtomAnnotation.of(Atom1)).build());
        QueryNode n4i = new QueryNode(e2, CQuery.with(new Triple(y, p1, Bob))
                .annotate(y, asRequired(Atom1, "Atom1"))
                .annotate(Bob, AtomAnnotation.of(Person)).build());
        QueryNode n5i = new QueryNode(e2, CQuery.with(new Triple(y, p2, Bob))
                .annotate(y, asRequired(Atom1, "Atom1"))
                .annotate(Bob, AtomAnnotation.of(Person)).build());

        // n*j :: OBJ is input
        QueryNode n1j = new QueryNode(e3, CQuery.with(new Triple(Alice, p1, x))
                .annotate(Alice, AtomAnnotation.of(Person))
                .annotate(x, asRequired(Atom1, "Atom1")).build());
        QueryNode n2j = new QueryNode(e3, CQuery.with(new Triple(x, p1, y))
                .annotate(x, AtomAnnotation.of(Atom1))
                .annotate(y, asRequired(Atom1, "Atom1")).build());
        QueryNode n5j = new QueryNode(e3, CQuery.with(new Triple(y, p2, Bob))
                .annotate(y, AtomAnnotation.of(Atom1))
                .annotate(Bob, asRequired(Person, "Person")).build());

        // mXi == M(nX, nXi)
        MultiQueryNode m1i = m(n1, n1i);

        IndexedSet<PlanNode> nodes = IndexedSet.fromDistinct(
                asList(n1, n2, n3, n4, n5, n6, n1i, n2i, n4i, n5i, n1j, n2j, n5j, m1i));

        return Stream.of(
                asList(createQuery(Alice, p1, x, x, p1, y),
                        asList(n1, n2),
                        singleton(new JoinComponent(nodes, n1, n2))),
                asList(createQuery(Alice, p1, x, x, p1, y),
                        singletonList(n3),
                        singleton(new JoinComponent(nodes, n3))),
                asList(createQuery(Alice, p1, x, x, p1, y),
                        asList(n1, n2, n3),
                        asList(new JoinComponent(nodes, n1, n2),
                               new JoinComponent(nodes, n3))),

                // n1 -> n2 --> n4
                //          +-> n4i
                asList(createQuery(Alice, p1, x, x, p1, y, y, p1, Bob),
                        asList(n1, n2, n4, n4i),
                        asList(new JoinComponent(nodes, n1, n2, n4),
                               new JoinComponent(nodes, n1, n2, n4i))),

                // m1i +-> n2  --+         +--> n5
                //     |         +--> n4 --+
                //     +-> n2i --+         +--> n5i
                //
                asList(createQuery(Alice, p1, x   , x, p1, y  ,
                                   y,     p1, Bob , y, p2, Bob),
                        asList(m1i, n2, n4, n5, n2i, n5i),
                        asList(new JoinComponent(nodes, m1i, n2, n4, n5),
                               new JoinComponent(nodes, m1i, n2, n4, n5i),
                               new JoinComponent(nodes, m1i, n2i, n4, n5),
                               new JoinComponent(nodes, m1i, n2i, n4, n5i)
                        )),

                // n1j <- n2j <--+          +--  n5j
                //         |     |          |
                //   +-----+     +--> n4 <--+
                //   v           |          |
                // n1i -> n2i  --+          +--> n5i
                asList(createQuery(Alice, p1, x  , x, p1, y  ,
                                   y,     p1, Bob, y, p2, Bob),
                        asList(n1j, n2j, n4, n5j, n1i, n2i, n5i),
                        asList(new JoinComponent(nodes, n1i, n2i, n4, n5i),
                               new JoinComponent(nodes, n1i, n2i, n4, n5j),

                               new JoinComponent(nodes, n5j, n4, n2j, n1j),
                               new JoinComponent(nodes, n5j, n4, n2j, n1i),

                               new JoinComponent(nodes, n4, n5i, n2j, n1j),
                               new JoinComponent(nodes, n4, n5i, n2j, n1i)
                        )),

                //  +------+------+
                //  |      |      |
                //  v      v      v
                // n1 <-> n2 <-> n6
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, x),
                       asList(n1, n2, n6),
                       singletonList(new JoinComponent(nodes, n1, n2, n6))),

                //  +------+------+
                //  |      |      |
                //  v      v      |
                // n1j <- n2j <- n6
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, x),
                        asList(n1j, n2j, n6),
                        singletonList(new JoinComponent(nodes, n1j, n2j, n6))),

                //         +-------+
                //  +------|------+|
                //  |      |      ||
                //  v      v      v|
                // n1i -> n2i -> n6i
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, x),
                        asList(n1i, n2i, n6),
                        singletonList(new JoinComponent(nodes, n1i, n2i, n6))),

                //          +----n5j
                //          v
                //  n1j <- n2 -> n5i
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, Bob),
                       asList(n1j, n2, n5i, n5j),
                       asList(new JoinComponent(nodes, n2, n1j, n5i),
                              new JoinComponent(nodes, n5j, n2, n1j)))
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "pathsData")
    public void testPaths(CQuery query, List<PlanNode> nodes, Collection<JoinComponent> expectedPaths) {
        double sum = 0;
        int count = 0;
        for (List<PlanNode> permutation : permutations(nodes)) {
            JoinGraph g = new JoinGraph(IndexedSet.fromDistinct(permutation));
            JoinPathsPlanner planner = new JoinPathsPlanner(new ArbitraryJoinOrderPlanner());
            Stopwatch sw = Stopwatch.createStarted();
            List<JoinComponent> paths = planner.getPaths(fromDistinctCopy(query.getMatchedTriples()), g);
            sum += sw.elapsed(TimeUnit.MICROSECONDS)/1000.0;
            ++count;

            List<JoinComponent> e = emptyList();
//            assertEquals(paths.stream().filter(JoinComponent::hasJoins)
//                    .map(JoinComponent::getJoinInfos)
//                    .filter(this::isBroken).collect(toList()), e);

            HashSet<JoinComponent> exSet = new HashSet<>(expectedPaths);

            assertEquals(paths.stream().filter(p -> !exSet.contains(p)).collect(toList()), e,
                         "There are unexpected paths");
            assertEquals(exSet.stream().filter(p -> !paths.contains(p)).collect(toList()), e,
                         "There are missing paths");
            assertEquals(new HashSet<>(paths), exSet);
        }
        sum /= count;
        System.out.printf("Average ms: %.3f\n", sum);
    }

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
        JoinPathsPlanner planner = new JoinPathsPlanner(new ArbitraryJoinOrderPlanner());
        List<IndexedSet<Triple>> list = planner.getCartesianComponents(IndexedSet.from(triples));
        assertEquals(new HashSet<>(list), expected.stream().map(IndexedSet::from).collect(toSet()));
    }

    @DataProvider
    public static @Nonnull Object[][] indexedSetForDuplicatesData() {
        QueryNode n1 = node(e1, Alice, p1, x  ), n1a = node(e1a, Alice, p1, x  );
        QueryNode n2 = node(e1, x,     p2, y  ), n2a = node(e1a, x,     p2, y  );
        QueryNode n3 = node(e1, y,     p3, z  ), n3a = node(e1a, y,     p3, z  );
        QueryNode n4 = node(e1, z,     p4, Bob), n4a = node(e1a, z,     p4, Bob);

        QueryNode n1i = node(e1, b -> b.annotate(Alice, asRequired(Person, "Person")), Alice, p1, x);
        QueryNode n2i = node(e1, b -> b.annotate(x, asRequired(Person, "Person")), x, p2, y);
        QueryNode n3i = node(e1, b -> b.annotate(y, asRequired(Person, "Person")), y, p3, z);
        QueryNode n4i = node(e1, b -> b.annotate(z, asRequired(Person, "Person")), z, p4, Bob);

        IndexedSet<PlanNode> all = IndexedSet.from(asList(n1 , n2 , n3 , n4 ,
                                                          n1a, n2a, n3a, n4a,
                                                          n1i, n2i, n3i, n4i));

        return Stream.of(
                emptyList(),
                singletonList(new JoinComponent(all, n1, n2, n3, n4)),
                singletonList(new JoinComponent(all, n1a, n2a, n3, n4)),
                asList(new JoinComponent(all, n1, n2),
                       new JoinComponent(all, n1a, n2a)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1a, n2a, n3a)),
                asList(new JoinComponent(all, n1, n2, n3, n4),
                       new JoinComponent(all, n1a, n2a, n3a, n4a)),
                asList(new JoinComponent(all, n1, n2, n3, n4),
                       new JoinComponent(all, n1a, n2a, n3a, n4a),
                       new JoinComponent(all, n1i, n2i, n3i, n4i)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2, n3a)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2, n3i)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2a, n3)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2i, n3))
        ).map(l -> new Object[] {l}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "indexedSetForDuplicatesData")
    public void testIndexedSetForDuplicates(List<JoinComponent> paths) {
        assertTrue(paths.stream().noneMatch(Objects::isNull));
        List<JoinComponent> oldPaths = new ArrayList<>(paths);

        JoinPathsPlanner planner = new JoinPathsPlanner(new ArbitraryJoinOrderPlanner());
        IndexedSet<PlanNode> set = planner.getNodesIndexedSetFromPaths(paths);

        //all nodes are in set
        List<PlanNode> missingNonQueryNodes = paths.stream().flatMap(p -> p.getNodes().stream())
                .filter(n -> !(n instanceof QueryNode) && !set.contains(n)).collect(toList());
        assertEquals(missingNonQueryNodes, emptyList());

        //no changes to the paths themselves
        assertEquals(paths.stream().filter(p -> !oldPaths.contains(p)).collect(toList()),
                     emptyList());

        // no equivalent endpoints for the same query
        for (int i = 0; i < set.size(); i++) {
            if (!(set.get(i) instanceof QueryNode)) continue;
            QueryNode outer = (QueryNode) set.get(i);
            for (int j = i+1; j < set.size(); j++) {
                if (!(set.get(j) instanceof QueryNode)) continue;
                QueryNode inner = (QueryNode) set.get(j);
                if (outer.getQuery().getSet().equals(inner.getQuery().getSet())) {
                    assertFalse(outer.getEndpoint().isAlternative(inner.getEndpoint()));
                    assertFalse(inner.getEndpoint().isAlternative(outer.getEndpoint()));
                }
            }
        }

        // can subset any QueryNode
        List<IndexedSubset<PlanNode>> singletons = paths.stream()
                .flatMap(p -> p.getNodes().stream())
                .filter(n -> n instanceof QueryNode)
                .map(set::subset).collect(toList());
        assertTrue(singletons.stream().noneMatch(IndexedSubset::isEmpty));
        assertTrue(singletons.stream().allMatch(s -> s.size() == 1));
    }

    @DataProvider
    public static @Nonnull Object[][] removeAlternativePathsData() {
        QueryNode n1   = node(e1,  Alice, knows, x);
        QueryNode n1a  = node(e1a, Alice, knows, x);
        QueryNode n1b  = node(e2,  Alice, knows, x);
        QueryNode n1i  = node(e1,  b -> b.annotate(x, asRequired(Person, "Person")),
                                   Alice, knows, x);
        QueryNode n1ai = node(e1a, b -> b.annotate(x, asRequired(Person, "Person")),
                                   Alice, knows, x);

        QueryNode n2   = node(e1,  x, knows, y);
        QueryNode n2a  = node(e1a, x, knows, y);
        QueryNode n2i  = node(e1a, b -> b.annotate(x, asRequired(Person, "Person")),
                                   x, knows, y);
        QueryNode n2ai = node(e1a, b -> b.annotate(x, asRequired(Person, "Person")),
                                   x, knows, y);


        return Stream.of(
                asList(singletonList(singleton(n1 )), emptyList()),
                asList(singletonList(singleton(n1a)), emptyList()),
                asList(singletonList(asList(n1, n2)), emptyList()),
                asList(asList(asList(n1, n2), asList(n1a, n2)), singletonList(asList(0, 1))),
                asList(asList(asList(n1, n2), asList(n1i, n2)), singletonList(asList(0, 1))),
                asList(asList(asList(n1, n2), asList(n1a, n2), asList(n1i, n2)),
                       singletonList(asList(0, 1, 2))),
                asList(asList(asList(n1, n2), asList(n1a, n2), asList(n1i, n2), asList(n1ai, n2)),
                       singletonList(asList(0, 1, 2, 3))),
                asList(asList(asList(n1, n2), asList(n1b, n2)), emptyList()),
                asList(asList(asList(n1i, n2), asList(n1b, n2)), emptyList()),
                asList(asList(asList(n1a, n2), asList(n1b, n2)), emptyList()),
                asList(asList(asList(n1, n2), asList(n1b, n2), asList(n1ai, n2)),
                       singletonList(asList(0, 2))),
                asList(asList(asList(n1b, n2 ), asList(n1b, n2a ),
                              asList(n1b, n2i), asList(n1b, n2ai)),
                       singletonList(asList(0, 1, 2, 3))),
                asList(asList(asList(n1,  n2i), asList(n1a, n2ai),
                              asList(n1b, n2 ), asList(n1b, n2a )),
                       asList(asList(0, 1), asList(2, 3)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "removeAlternativePathsData")
    public void testRemoveAlternativePaths(List<Collection<PlanNode>> nodesList,
                                           List<List<Integer>> equivIndices) {
        JoinPathsPlanner planner = new JoinPathsPlanner(new ArbitraryJoinOrderPlanner());
        //setup
        IndexedSet<PlanNode> nodes = IndexedSet.fromDistinct(
                nodesList.stream().flatMap(Collection::stream).collect(toSet()));
        JoinGraph graph = new JoinGraph(nodes);
        List<JoinComponent> pathsList;
        pathsList = nodesList.stream().map(n -> new JoinComponent(graph, n)).collect(toList());
        List<JoinComponent> origPaths = new ArrayList<>(pathsList);

        //sanity
        assertEquals(new HashSet<>(pathsList).size(), nodesList.size());
        for (int i = 0; i < pathsList.size(); i++) {
            // all nodes in path
            assertEquals(pathsList.get(i).getNodes(), new HashSet<>(nodesList.get(i)));

            // alternatives cannot be in the same JoinPath
            List<PlanNode> pathNodes = new ArrayList<>(pathsList.get(i).getNodes());
            for (int j = 0; j < pathNodes.size(); j++) {
                if (!(pathNodes.get(j) instanceof QueryNode)) continue;
                QueryNode outer = (QueryNode) pathNodes.get(j);
                for (int k = j+1; k < pathNodes.size(); k++) {
                    if (!(pathNodes.get(k) instanceof QueryNode)) continue;
                    QueryNode inner = (QueryNode) pathNodes.get(k);
                    if (outer.getQuery().getSet().equals(inner.getQuery().getSet())) {
                        assertFalse(inner.getEndpoint().isAlternative(outer.getEndpoint()));
                        assertFalse(outer.getEndpoint().isAlternative(inner.getEndpoint()));
                    }
                }
            }
        }
        assertTrue(pathsList.stream().noneMatch(Objects::isNull));


        //operation & checks
        planner.removeAlternativePaths(pathsList);
        for (List<Integer> list : equivIndices) {
            Set<JoinComponent> set = list.stream().map(origPaths::get).collect(toSet());
            set.retainAll(pathsList);
            assertEquals(set.size(), 1); // exactly one path must remain
        }

        List<Integer> missing = IntStream.range(0, origPaths.size()).boxed()
                .filter(i -> equivIndices.stream().noneMatch(l -> l.contains(i)))
                .filter(i -> !pathsList.contains(origPaths.get(i)))
                .collect(toList());
        assertEquals(missing, emptyList());
    }

}