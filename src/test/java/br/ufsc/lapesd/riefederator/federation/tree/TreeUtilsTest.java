package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;

public class TreeUtilsTest {
    public static final URI ALICE = new StdURI("http://example.org/Alice");
    public static final URI BOB = new StdURI("http://example.org/Bob");
    public static final URI CHARLIE = new StdURI("http://example.org/Charlie");
    public static final URI DAVE = new StdURI("http://example.org/Dave");
    public static final URI knows = new StdURI(FOAF.knows.getURI());
    public static final Var X = new StdVar("x");
    public static final Var Y = new StdVar("y");
    public static final Var Z = new StdVar("z");
    private static EmptyEndpoint ep = new EmptyEndpoint();

    @DataProvider
    public static Object[][] iterateDeepLeft() {
        QueryNode q1 = new QueryNode(ep, CQuery.from(new Triple(X, knows, ALICE)));
        QueryNode q2 = new QueryNode(ep, CQuery.from(new Triple(X, knows, Y)));
        JoinNode j1 = JoinNode.builder(q1, q2).build();
        QueryNode q3 = new QueryNode(ep, CQuery.from(new Triple(Y, knows, CHARLIE)));
        QueryNode q4 = new QueryNode(ep, CQuery.from(new Triple(Y, knows, DAVE)));
        JoinNode j2 = JoinNode.builder(q3, q4).build();
        JoinNode j3 = JoinNode.builder(j1, j2).build();
        QueryNode q5 = new QueryNode(ep, CQuery.from(new Triple(Z, knows, ALICE)));
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

        List<PlanNode> streamed = TreeUtils.streamPreOrder(root).collect(Collectors.toList());
        assertEquals(streamed, expected);
    }

    @DataProvider
    public static Object[][] intersectResultsData() {
        QueryNode x = new QueryNode(ep, CQuery.from(new Triple(X, knows, ALICE)));
        QueryNode xy = new QueryNode(ep, CQuery.from(new Triple(X, knows, Y)));
        QueryNode z = new QueryNode(ep, CQuery.from(new Triple(ALICE, Z, BOB)));
        QueryNode xyz = new QueryNode(ep, CQuery.from(new Triple(X, Y, Z)));

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
        QueryNode x = new QueryNode(ep, CQuery.from(new Triple(X, knows, ALICE)));
        QueryNode xy = new QueryNode(ep, CQuery.from(new Triple(X, knows, Y)));
        QueryNode z = new QueryNode(ep, CQuery.from(new Triple(ALICE, Z, BOB)));
        QueryNode xyz = new QueryNode(ep, CQuery.from(new Triple(X, Y, Z)));

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

        QueryNode xInYZOut = new QueryNode(ep, CQuery.with(new Triple(X, Y, Z))
                .annotate(X, AtomAnnotation.asRequired(atom1))
                .build());
        QueryNode xyInZOut = new QueryNode(ep, CQuery.with(new Triple(X, Y, Z))
                .annotate(X, AtomAnnotation.asRequired(atom1))
                .annotate(Y, AtomAnnotation.asRequired(atom2))
                .build());
        QueryNode xKnowsALICE = new QueryNode(ep, CQuery.from(new Triple(X, knows, ALICE)));
        QueryNode xKnowsZ = new QueryNode(ep, CQuery.from(new Triple(X, knows, Z)));
        QueryNode xKnowsY = new QueryNode(ep, CQuery.from(new Triple(X, knows, Y)));

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