package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.isAcyclic;
import static br.ufsc.lapesd.riefederator.query.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public class TreeUtilsTest implements TestContext {
    private static EmptyEndpoint ep = new EmptyEndpoint();
    private final Atom person = Molecule.builder("Person").buildAtom();

    @DataProvider
    public static Object[][] iterateDeepLeft() {
        QueryNode q1 = new QueryNode(ep, createQuery(x, knows, Alice));
        QueryNode q2 = new QueryNode(ep, createQuery(x, knows, y));
        JoinNode j1 = JoinNode.builder(q1, q2).build();
        QueryNode q3 = new QueryNode(ep, createQuery(y, knows, Charlie));
        QueryNode q4 = new QueryNode(ep, createQuery(y, knows, Dave));
        JoinNode j2 = JoinNode.builder(q3, q4).build();
        JoinNode j3 = JoinNode.builder(j1, j2).build();
        QueryNode q5 = new QueryNode(ep, createQuery(z, knows, Alice));
        CartesianNode c1 = new CartesianNode(asList(j3, q5));

        return new Object[][] {
                new Object[] {q1, singletonList(q1)},
                new Object[] {j1, asList(j1, q1, q2)},
                new Object[] {j3, asList(j3, j1, q1, q2, j2, q3, q4)},
                new Object[] {c1, asList(c1, j3, j1, q1, q2, j2, q3, q4, q5)},
        };
    }

    @Test(dataProvider = "iterateDeepLeft")
    public void testDeepLeft(@Nonnull PlanNode root, @Nonnull List<PlanNode> expected) {
        List<PlanNode> iterated = new ArrayList<>();
        Iterator<PlanNode> it = TreeUtils.iteratePreOrder(root);
        while (it.hasNext())
            iterated.add(it.next());
        assertEquals(iterated, expected);

        List<PlanNode> streamed = TreeUtils.streamPreOrder(root).collect(toList());
        assertEquals(streamed, expected);
    }

    @Test
    public void testIsTreeForgivesQueryNodes() {
        QueryNode q1  = new QueryNode(ep, createQuery(x, knows, Alice));
        QueryNode q2  = new QueryNode(ep, createQuery(x, knows, Bob));
        QueryNode q1a = new QueryNode(ep, CQuery.with(new Triple(x, knows, Alice))
                .annotate(x, asRequired(person, "person"))
                .build());
        JoinNode j1 = JoinNode.builder(q1,  q2).build();
        JoinNode j2 = JoinNode.builder(q1a, q2).build();
        MultiQueryNode root = MultiQueryNode.builder().add(j1).add(j2).build();

        assertFalse(TreeUtils.isTree(root, false));
        assertTrue(TreeUtils.isTree(root, true));
        assertFalse(TreeUtils.isTree(root));
        assertTrue(isAcyclic(root));
    }

    @Test
    public void testIsAcyclicSimple() {
        QueryNode n1 = new QueryNode(ep, createQuery(Alice, knows, x));
        QueryNode n2 = new QueryNode(ep, createQuery(x, knows, Bob));
        JoinNode root = JoinNode.builder(n1, n2).build();

        assertTrue(isAcyclic(n1));
        assertTrue(isAcyclic(root));
    }

    @Test
    public void testIsAcyclicWithQueryNodeReuse() {
        QueryNode n1 = new QueryNode(ep, createQuery(Alice, knows, x));
        QueryNode n2 = new QueryNode(ep, createQuery(x, knows, Bob));
        JoinNode j1 = JoinNode.builder(n1, n2).build();
        JoinNode j2 = JoinNode.builder(n1, n2).build();
        MultiQueryNode r = MultiQueryNode.builder().add(j1).add(j2).build();

        assertEquals(Stream.of(j1,j2,r).filter(n -> !isAcyclic(n)).collect(toList()), emptyList());
    }

    @Test
    public void testIsAcyclicWithJoinNodeReuse() {
        EmptyEndpoint ep2 = new EmptyEndpoint();
        QueryNode n1 = new QueryNode(ep, createQuery(Alice, knows, x));
        QueryNode n2 = new QueryNode(ep, createQuery(x, knows, y));
        QueryNode n3a = new QueryNode(ep , createQuery(y, knows, Bob));
        QueryNode n3b = new QueryNode(ep2, createQuery(y, knows, Bob));
        JoinNode j1 = JoinNode.builder(n1, n2).build();
        JoinNode j2 = JoinNode.builder(j1, n3a).build();
        JoinNode j3 = JoinNode.builder(j1, n3b).build();
        MultiQueryNode root = MultiQueryNode.builder().add(j2).add(j3).build();

        assertEquals(Stream.of(j1,j2,j3,root).filter(n -> !isAcyclic(n)).collect(toList()),
                     emptyList());
    }

    @DataProvider
    public static Object[][] intersectResultsData() {
        QueryNode x = new QueryNode(ep, createQuery(TreeUtilsTest.x, knows, Alice));
        QueryNode xy = new QueryNode(ep, createQuery(TreeUtilsTest.x, knows, y));
        QueryNode z = new QueryNode(ep, createQuery(Alice, TreeUtilsTest.z, Bob));
        QueryNode xyz = new QueryNode(ep, createQuery(TreeUtilsTest.x, y, TreeUtilsTest.z));

        return new Object[][] {
                new Object[] {emptyList(), emptySet(), false},
                new Object[] {singleton(x), singleton("x"), false},
                new Object[] {singleton(xyz), asList("x", "y", "z"), false},
                new Object[] {asList(xyz, xyz), asList("x", "y", "z"), false},
                new Object[] {asList(x, x), singleton("x"), false},
                new Object[] {asList(x, z), emptyList(), true},
                new Object[] {asList(z, xy), emptyList(), true},
                new Object[] {asList(x, xy), singleton("x"), true},
                new Object[] {asList(xy, x), singleton("x"), true},
                new Object[] {asList(xyz, xy), asList("x", "y"), true},

                new Object[] {asList(x, z, xyz), emptyList(), true},
                new Object[] {asList(x, xyz, z), emptyList(), true},
                new Object[] {asList(xyz, x, z), emptyList(), true},

                new Object[] {asList(x, xy, xyz), singleton("x"), true},
                new Object[] {asList(x, xyz, xy), singleton("x"), true},
                new Object[] {asList(xyz, x, xy), singleton("x"), true},
        };
    }

    @Test(dataProvider = "intersectResultsData")
    public void testIntersectResults(@Nonnull Collection<PlanNode> list,
                                     @Nonnull Collection<String> expected, boolean dropExpected) {
        AtomicBoolean dropped = new AtomicBoolean();
        Set<String> actual = TreeUtils.intersectResults(list, dropped);
        assertEquals(actual, new HashSet<>(expected));
        assertEquals(dropped.get(), dropExpected);
    }

    @DataProvider
    public static Object[][] unionResultsData() {
        QueryNode x = new QueryNode(ep, createQuery(TreeUtilsTest.x, knows, Alice));
        QueryNode xy = new QueryNode(ep, createQuery(TreeUtilsTest.x, knows, y));
        QueryNode z = new QueryNode(ep, createQuery(Alice, TreeUtilsTest.z, Bob));
        QueryNode xyz = new QueryNode(ep, createQuery(TreeUtilsTest.x, y, TreeUtilsTest.z));

        return new Object[][] {
                new Object[] {emptyList(), emptySet()},
                new Object[] {singleton(x), singleton("x")},
                new Object[] {singleton(xyz), asList("x", "y", "z")},
                new Object[] {asList(xyz, xyz), asList("x", "y", "z")},
                new Object[] {asList(x, x), singleton("x")},
                new Object[] {asList(x, z), asList("x", "z")},
                new Object[] {asList(x, xy), asList("x", "y")},
                new Object[] {asList(xy, x), asList("x", "y")},
                new Object[] {asList(xy, x, z), asList("x", "y", "z")},
        };
    }

    @Test(dataProvider = "unionResultsData")
    public void testUnionResults(@Nonnull Collection<PlanNode> list,
                                 @Nonnull Collection<String> expected) {
        assertEquals(TreeUtils.unionResults(list), new HashSet<>(expected));
    }

    @Test
    public void testBinaryIntersect() {
        List<String> xy = asList("x", "y");
        List<String> xyz = asList("x", "y", "z");
        Set<String> x = singleton("x");

        assertEquals(TreeUtils.intersect(xy, x), Sets.newHashSet("x"));
        assertEquals(TreeUtils.intersect(x, xy), Sets.newHashSet("x"));
        assertEquals(TreeUtils.intersect(xyz, xy), Sets.newHashSet("x", "y"));
        assertEquals(TreeUtils.intersect(xy, xyz), Sets.newHashSet("x", "y"));
        assertEquals(TreeUtils.intersect(xyz, xyz), Sets.newHashSet("x", "y", "z"));
    }

    @DataProvider
    public static Object[][] joinVarsData() {
        Atom atom1 = new Atom("Atom1");
        Atom atom2 = new Atom("Atom2");

        QueryNode xInYZOut = new QueryNode(ep, CQuery.with(new Triple(x, y, z))
                .annotate(x, asRequired(atom1, "atom1"))
                .build());
        QueryNode xyInZOut = new QueryNode(ep, CQuery.with(new Triple(x, y, z))
                .annotate(x, asRequired(atom1, "atom1"))
                .annotate(y, asRequired(atom2, "atom2"))
                .build());
        QueryNode xKnowsALICE = new QueryNode(ep, createQuery(x, knows, Alice));
        QueryNode xKnowsZ = new QueryNode(ep, createQuery(x, knows, z));
        QueryNode xKnowsY = new QueryNode(ep, createQuery(x, knows, y));

        return new Object[][] {
                new Object[]{xKnowsALICE, xKnowsZ, singleton("x"), emptyList()},
                new Object[]{xKnowsALICE, xInYZOut, singleton("x"), emptyList()},
                new Object[]{xKnowsALICE, xyInZOut, singleton("x"), singleton("y")},
                new Object[]{xKnowsZ, xInYZOut, singleton("x"), emptyList()},
                new Object[]{xKnowsZ, xyInZOut, singleton("x"), singleton("y")},
                new Object[]{xKnowsY, xyInZOut, asList("x", "y"), emptyList()},
                new Object[]{xKnowsY, xInYZOut, singleton("x"), emptyList()},
                new Object[]{xInYZOut, xyInZOut, emptySet(), emptyList()},
        };
    }

    @Test(dataProvider = "joinVarsData")
    public void testJoinVars(@Nonnull PlanNode l, @Nonnull PlanNode r,
                             @Nonnull Collection<String> join, @Nonnull Collection<String> pending){
        assertEquals(TreeUtils.joinVars(l, r), new HashSet<>(join));
        assertEquals(TreeUtils.joinVars(r, l), new HashSet<>(join));

        Set<String> actualPending = new HashSet<>();
        assertEquals(TreeUtils.joinVars(l, r, actualPending), new HashSet<>(join));
        assertEquals(actualPending, new HashSet<>(pending));

        assertEquals(TreeUtils.joinVars(r, l, actualPending), new HashSet<>(join));
        assertEquals(actualPending, new HashSet<>(pending));
    }

}