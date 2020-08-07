package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.isAcyclic;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class TreeUtilsTest implements TestContext {
    private static final EmptyEndpoint ep = new EmptyEndpoint();
    private final Atom person = Molecule.builder("Person").buildAtom();

    @DataProvider
    public static Object[][] iterateDeepLeft() {
        QueryOp q1 = new QueryOp(ep, createQuery(x, knows, Alice));
        QueryOp q2 = new QueryOp(ep, createQuery(x, knows, y));
        JoinOp j1 = JoinOp.create(q1, q2);
        QueryOp q3 = new QueryOp(ep, createQuery(y, knows, Charlie));
        QueryOp q4 = new QueryOp(ep, createQuery(y, knows, Dave));
        JoinOp j2 = JoinOp.create(q3, q4);
        JoinOp j3 = JoinOp.create(j1, j2);
        QueryOp q5 = new QueryOp(ep, createQuery(z, knows, Alice));
        CartesianOp c1 = new CartesianOp(asList(j3, q5));

        return new Object[][] {
                new Object[] {q1, singletonList(q1)},
                new Object[] {j1, asList(j1, q1, q2)},
                new Object[] {j3, asList(j3, j1, q1, q2, j2, q3, q4)},
                new Object[] {c1, asList(c1, j3, j1, q1, q2, j2, q3, q4, q5)},
        };
    }

    @Test(dataProvider = "iterateDeepLeft")
    public void testDeepLeft(@Nonnull Op root, @Nonnull List<Op> expected) {
        List<Op> iterated = new ArrayList<>();
        Iterator<Op> it = TreeUtils.iteratePreOrder(root);
        while (it.hasNext())
            iterated.add(it.next());
        assertEquals(iterated, expected);

        List<Op> streamed = TreeUtils.streamPreOrder(root).collect(toList());
        assertEquals(streamed, expected);
    }

    @Test
    public void testIsTreeForgivesQueryNodes() {
        QueryOp q1  = new QueryOp(ep, createQuery(x, knows, Alice));
        QueryOp q2  = new QueryOp(ep, createQuery(x, knows, Bob));
        QueryOp q1a = new QueryOp(ep, createQuery(
                x, AtomInputAnnotation.asRequired(person, "person").get(),
                        knows, Alice));
        JoinOp j1 = JoinOp.create(q1,  q2);
        JoinOp j2 = JoinOp.create(q1a, q2);
        UnionOp root = UnionOp.builder().add(j1).add(j2).build();

        assertFalse(TreeUtils.isTree(root, false));
        assertTrue(TreeUtils.isTree(root, true));
        assertFalse(TreeUtils.isTree(root));
        assertTrue(isAcyclic(root));
    }

    @Test
    public void testIsAcyclicSimple() {
        QueryOp n1 = new QueryOp(ep, createQuery(Alice, knows, x));
        QueryOp n2 = new QueryOp(ep, createQuery(x, knows, Bob));
        JoinOp root = JoinOp.create(n1, n2);

        assertTrue(isAcyclic(n1));
        assertTrue(isAcyclic(root));
    }

    @Test
    public void testIsAcyclicWithQueryNodeReuse() {
        QueryOp n1 = new QueryOp(ep, createQuery(Alice, knows, x));
        QueryOp n2 = new QueryOp(ep, createQuery(x, knows, Bob));
        JoinOp j1 = JoinOp.create(n1, n2);
        JoinOp j2 = JoinOp.create(n1, n2);
        UnionOp r = UnionOp.builder().add(j1).add(j2).build();

        assertEquals(Stream.of(j1,j2,r).filter(n -> !isAcyclic(n)).collect(toList()), emptyList());
    }

    @Test
    public void testIsAcyclicWithJoinNodeReuse() {
        EmptyEndpoint ep2 = new EmptyEndpoint();
        QueryOp n1 = new QueryOp(ep, createQuery(Alice, knows, x));
        QueryOp n2 = new QueryOp(ep, createQuery(x, knows, y));
        QueryOp n3a = new QueryOp(ep , createQuery(y, knows, Bob));
        QueryOp n3b = new QueryOp(ep2, createQuery(y, knows, Bob));
        JoinOp j1 = JoinOp.create(n1, n2);
        JoinOp j2 = JoinOp.create(j1, n3a);
        JoinOp j3 = JoinOp.create(j1, n3b);
        UnionOp root = UnionOp.builder().add(j2).add(j3).build();

        assertEquals(Stream.of(j1,j2,j3,root).filter(n -> !isAcyclic(n)).collect(toList()),
                     emptyList());
    }

    @DataProvider
    public static Object[][] intersectResultsData() {
        QueryOp x = new QueryOp(ep, createQuery(TreeUtilsTest.x, knows, Alice));
        QueryOp xy = new QueryOp(ep, createQuery(TreeUtilsTest.x, knows, y));
        QueryOp z = new QueryOp(ep, createQuery(Alice, TreeUtilsTest.z, Bob));
        QueryOp xyz = new QueryOp(ep, createQuery(TreeUtilsTest.x, y, TreeUtilsTest.z));

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
    public void testIntersectResults(@Nonnull Collection<Op> list,
                                     @Nonnull Collection<String> expected, boolean dropExpected) {
        AtomicBoolean dropped = new AtomicBoolean();
        Set<String> actual = CollectionUtils.intersect(list, Op::getResultVars, dropped);
        assertEquals(actual, new HashSet<>(expected));
        assertEquals(dropped.get(), dropExpected);
    }

    @DataProvider
    public static Object[][] unionResultsData() {
        QueryOp x = new QueryOp(ep, createQuery(TreeUtilsTest.x, knows, Alice));
        QueryOp xy = new QueryOp(ep, createQuery(TreeUtilsTest.x, knows, y));
        QueryOp z = new QueryOp(ep, createQuery(Alice, TreeUtilsTest.z, Bob));
        QueryOp xyz = new QueryOp(ep, createQuery(TreeUtilsTest.x, y, TreeUtilsTest.z));

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
    public void testUnionResults(@Nonnull Collection<Op> list,
                                 @Nonnull Collection<String> expected) {

        assertEquals(CollectionUtils.union(list, Op::getResultVars), new HashSet<>(expected));
    }

    @Test
    public void testBinaryIntersect() {
        List<String> xy = asList("x", "y");
        List<String> xyz = asList("x", "y", "z");
        Set<String> x = singleton("x");

        assertEquals(CollectionUtils.intersect(xy, x), newHashSet("x"));
        assertEquals(CollectionUtils.intersect(x, xy), newHashSet("x"));
        assertEquals(CollectionUtils.intersect(xyz, xy), newHashSet("x", "y"));
        assertEquals(CollectionUtils.intersect(xy, xyz), newHashSet("x", "y"));
        assertEquals(CollectionUtils.intersect(xyz, xyz), newHashSet("x", "y", "z"));
    }

    @DataProvider
    public static Object[][] joinVarsData() {
        Atom atom1 = new Atom("Atom1");
        Atom atom2 = new Atom("Atom2");

        QueryOp xInYZOut = new QueryOp(ep, createQuery(
                x, AtomInputAnnotation.asRequired(atom1, "atom1").get(), y, z));
        QueryOp xyInZOut = new QueryOp(ep, createQuery(
                x, AtomInputAnnotation.asRequired(atom1, "atom1").get(),
                        y, z, AtomInputAnnotation.asRequired(atom2, "atom2").get()));
        QueryOp xKnowsALICE = new QueryOp(ep, createQuery(x, knows, Alice));
        QueryOp xKnowsZ = new QueryOp(ep, createQuery(x, knows, z));
        QueryOp xKnowsY = new QueryOp(ep, createQuery(x, knows, y));

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
}