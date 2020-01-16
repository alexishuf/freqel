package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.description.MatchAnnotation;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class HeuristicPlannerTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI name = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdURI mainName = new StdURI("http://example.org/mainName");
    public static final @Nonnull StdURI likes = new StdURI("http://example.org/likes");
    public static final @Nonnull StdURI author = new StdURI("http://example.org/author");
    public static final @Nonnull StdLit author1 = StdLit.fromUnescaped("author 1", "en");
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar Z = new StdVar("z");
    public static final @Nonnull StdVar U = new StdVar("u");
    public static final @Nonnull StdVar V = new StdVar("v");


    public static final @Nonnull EmptyEndpoint empty = new EmptyEndpoint();
    public static final @Nonnull EmptyEndpoint empty2 = new EmptyEndpoint();
    public QueryNode xKnowsY, aliceKnowsX, yKnowsBob;
    public QueryNode uKnowsV, aliceKnowsU, vKnowsBob;

    public static final @Nonnull Atom Book = new Atom("Book");
    public static final @Nonnull Atom Person = new Atom("Person");
    public static final @Nonnull Atom LikedPerson = new Atom("LikedPerson");
    public static final @Nonnull Atom PersonName = new Atom("PersonName");

    @BeforeMethod
    public void setUp() {
        xKnowsY = new QueryNode(empty, CQuery.from(new Triple(X, knows, Y)));
        aliceKnowsX = new QueryNode(empty, CQuery.from(new Triple(ALICE, knows, X)));
        yKnowsBob = new QueryNode(empty, CQuery.from(new Triple(Y, knows, BOB)));

        uKnowsV = new QueryNode(empty, CQuery.from(new Triple(U, knows, V)));
        aliceKnowsU = new QueryNode(empty, CQuery.from(new Triple(ALICE, knows, U)));
        vKnowsBob = new QueryNode(empty, CQuery.from(new Triple(V, knows, BOB)));
    }

    protected void checkIntersection(@Nonnull HeuristicPlanner.JoinGraph g,
                                     float[][] expected) {
        assertEquals(g.getIntersection().length, expected.length);
        for (int i = 0; i < expected.length; i++)
            assertEquals(g.getIntersection()[i], expected[i], "i=" + i);
    }

    @Test
    public void testTryJoinSimple() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY));
        float[][] intersection = new float[][] {
                new float[] {0, 1},
                new float[] {0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        assertTrue(g.tryJoin());
        assertFalse(g.tryJoin());

        PlanNode root = g.getLeaves().get(0);
        assertTrue(root instanceof JoinNode);
        assertFalse(root.isProjecting());
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(((JoinNode)root).getJoinVars(), singleton("x"));
    }

    @Test
    public void testJoinThreeHopPath() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        float[][] intersection = new float[][] {
                new float[] {0, 1, 0},
                new float[] {0, 0, 1},
                new float[] {0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        assertTrue(g.tryJoin());
        assertTrue(g.tryJoin());
        assertFalse(g.tryJoin());

        PlanNode root = g.getLeaves().get(0);
        assertTrue(root instanceof JoinNode);
        assertFalse(root.isProjecting());
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(((JoinNode)root).getJoinVars(), singleton("y"));

        PlanNode left = ((JoinNode) root).getLeft();
        assertTrue(left instanceof JoinNode);
        assertFalse(left.isProjecting());
        assertEquals(left.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(((JoinNode)left).getJoinVars(), singleton("x"));
    }

    @Test
    public void testJoinSingle() {
        ArrayList<PlanNode> leaves = new ArrayList<>(singletonList(aliceKnowsX));
        float[][] intersection = new float[][]{
                new float[]{0}
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        assertFalse(g.tryJoin());
        assertSame(g.getLeaves().get(0), leaves.get(0));
    }

    @Test
    public void testCreateTotalComponent() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY));
        float[][] intersection = new float[][] {
                new float[]{0, 1},
                new float[]{0, 0}
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        BitSet subset = new BitSet();
        subset.set(0, 2);
        HeuristicPlanner.JoinGraph component = g.createComponent(subset);

        assertEquals(component.getLeaves(), leaves);
        assertEquals(component.getIntersection().length, intersection.length);
        for (int i = 0; i < intersection.length; i++)
            assertEquals(component.getIntersection()[i], intersection[i]);
    }

    @Test
    public void testCreateUnitComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        float[][] intersection = new float[][] {
                new float[]{0, 1, 0},
                new float[]{0, 0, 1},
                new float[]{0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        for (int i = 0; i < leaves.size(); i++) {
            BitSet subset = new BitSet();
            subset.set(i);
            HeuristicPlanner.JoinGraph component = g.createComponent(subset);
            assertEquals(component.getLeaves(), singletonList(leaves.get(i)));
            assertEquals(component.getIntersection().length, 1);
            assertEquals(component.getIntersection()[0].length, 1);
            assertEquals(component.getIntersection()[0][0], 0.0f);
        }
    }

    @Test
    public void testCreatePairComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        float[][] intersection = {
                new float[] {0, 1, 0},
                new float[] {0, 0, 1},
                new float[] {0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        for (int i = 0; i < leaves.size(); i++) {
            BitSet subset = new BitSet();
            subset.set(i);
            subset.set((i+1) % leaves.size());
            HeuristicPlanner.JoinGraph component = g.createComponent(subset);

            if (i < leaves.size()-1)
                assertEquals(component.getLeaves(), asList(leaves.get(i), leaves.get(i+1)));
            else
                assertEquals(component.getLeaves(), asList(leaves.get(0), leaves.get(i)));

            assertEquals(component.getIntersection().length, 2);
            for (int j = 0; j < 2; j++)
                assertEquals(component.getIntersection()[j].length, 2, "j=" + j);

            if (i < leaves.size()-1)
                assertEquals(component.getIntersection()[0], new float[]{0, 1}, "i="+i);
            else
                assertEquals(component.getIntersection()[0], new float[]{0, 0}, "i="+i);
            assertEquals(component.getIntersection()[1], new float[]{0, 0}, "i="+i);
        }
    }

    @Test
    public void testCreateDisjointComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob,
                                                             aliceKnowsU, uKnowsV, vKnowsBob));
        float[][] intersection = {
                new float[] {0, 1, 0, 0, 0, 0},
                new float[] {0, 0, 1, 0, 0, 0},
                new float[] {0, 0, 0, 0, 0, 0},
                new float[] {0, 0, 0, 0, 1, 0},
                new float[] {0, 0, 0, 0, 0, 1},
                new float[] {0, 0, 0, 0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        BitSet subset = new BitSet();
        subset.set(0, 3);
        HeuristicPlanner.JoinGraph left = g.createComponent(subset);
        assertEquals(left.getLeaves(), asList(aliceKnowsX, xKnowsY, yKnowsBob));
        float[][] exComponent = {
                new float[] {0, 1, 0},
                new float[] {0, 0, 1},
                new float[] {0, 0, 0},
        };
        checkIntersection(left, exComponent);

        subset.clear();
        subset.set(3, 6);
        HeuristicPlanner.JoinGraph right = g.createComponent(subset);
        assertEquals(right.getLeaves(), asList(aliceKnowsU, uKnowsV, vKnowsBob));
        checkIntersection(right, exComponent);
    }

    @Test
    public void testJoinGraphWithServices() {
        QueryNode xKnowsBob = new QueryNode(empty, CQuery.from(new Triple(X, knows, BOB)));
        QueryNode xNameY = new QueryNode(empty, CQuery.with(new Triple(X, name, Y))
                .annotate(X, AtomAnnotation.asRequired(Person))
                .annotate(Y, AtomAnnotation.of(PersonName))
                .build());
        QueryNode xLikesZ = new QueryNode(empty, CQuery.with(new Triple(X, likes, Z))
                .annotate(X, AtomAnnotation.asRequired(Person))
                .annotate(Z, AtomAnnotation.of(LikedPerson))
                .build());
        List<QueryNode> nodes = asList(xKnowsBob, xNameY, xLikesZ);
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(nodes);

        float[][] expected = {
                new float[] {0, 1, 1},
                new float[] {0, 0, 0},
                new float[] {0, 0, 0},
        };
        checkIntersection(g, expected);
    }

    @Test
    public void testBuildTreeForServiceChain() {
        QueryNode q1 = new QueryNode(empty, CQuery.with(new Triple(ALICE, knows, X))
                .annotate(ALICE, AtomAnnotation.asRequired(Person))
                .annotate(X, AtomAnnotation.of(Person)).build());
        QueryNode q2 = new QueryNode(empty, CQuery.with(new Triple(X, knows, Y))
                .annotate(X, AtomAnnotation.asRequired(Person))
                .annotate(Y, AtomAnnotation.of(Person)).build());
        QueryNode q3 = new QueryNode(empty, CQuery.with(new Triple(Y, knows, Z))
                .annotate(Y, AtomAnnotation.asRequired(Person))
                .annotate(Z, AtomAnnotation.of(Person)).build());
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(asList(q1, q2, q3));

        checkIntersection(g, new float[][]{
                {0, 1, 0},
                {0, 0, 1},
                {0, 0, 0},
        });
        PlanNode rootNode = g.buildTree();
        assertNotNull(rootNode);
        assertFalse(rootNode.hasInputs());
        assertTrue(rootNode instanceof JoinNode);
        assertEquals(rootNode.getResultVars(), Sets.newHashSet("x", "y", "z"));

        assertTrue(rootNode.getChildren().get(0) instanceof JoinNode);
        JoinNode left = (JoinNode) rootNode.getChildren().get(0);
        assertSame(left.getLeft(), q1);
        assertSame(left.getRight(), q2);
        assertSame(rootNode.getChildren().get(1), q3);

        assertFalse(left.hasInputs());
    }

    @Test
    public void testBuildTreeWithInputsAtRoot() {
        QueryNode q1 = new QueryNode(empty, CQuery.with(new Triple(X, knows, Y))
                .annotate(X, AtomAnnotation.asRequired(Person))
                .annotate(Y, AtomAnnotation.of(Person)).build());
        QueryNode q2 = new QueryNode(empty, CQuery.with(new Triple(Y, knows, Z))
                .annotate(Y, AtomAnnotation.asRequired(Person))
                .annotate(Z, AtomAnnotation.of(Person)).build());

        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(asList(q1, q2));
        PlanNode root = g.buildTree();
        assertTrue(root instanceof EmptyNode);
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y", "z"));
        assertEquals(root.getChildren().size(), 0);
    }

    @Test
    public void testStartFromBoundNotFromBest() {
        QueryNode q1 = new QueryNode(empty, CQuery.with(new Triple(X, knows, Y),
                                                        new Triple(Y, likes, U))
                .annotate(X, AtomAnnotation.asRequired(Person))
                .annotate(Y, AtomAnnotation.of(Person))
                .annotate(U, AtomAnnotation.of(Person)).build());
        QueryNode q2 = new QueryNode(empty, CQuery.with(new Triple(U, knows, Y),
                                                        new Triple(Y, likes, V))
                .annotate(U, AtomAnnotation.asRequired(Person))
                .annotate(Y, AtomAnnotation.asRequired(Person))
                .annotate(V, AtomAnnotation.of(Person)).build());
        QueryNode q3 = new QueryNode(empty, CQuery.with(new Triple(ALICE, knows, X))
                .annotate(ALICE, AtomAnnotation.asRequired(Person))
                .annotate(X, AtomAnnotation.of(Person)).build());

        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(asList(q1, q2, q3));
        PlanNode root = g.buildTree();
        assertTrue(root instanceof JoinNode);

        PlanNode left = root.getChildren().get(0), right = root.getChildren().get(1);
        assertTrue(left instanceof JoinNode);
        assertTrue(Sets.newHashSet(left.getChildren()).contains(q3));
        assertTrue(right == q1 || right == q2);
        if (right == q1) assertTrue(left.getChildren().contains(q2));
        if (right == q2) assertTrue(left.getChildren().contains(q1));

        assertFalse(root.hasInputs());
        assertFalse(left.hasInputs());
    }

    @Test
    public void testAlternativeInterfacesNotJoinable() {
        QueryNode q1 = new QueryNode(empty, CQuery.from(new Triple(Y, name, author1)));
        QueryNode q2 = new QueryNode(empty, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.asRequired(Book))
                .annotate(Y, AtomAnnotation.of(Person)).build());
        QueryNode q3 = new QueryNode(empty, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.of(Book))
                .annotate(Y, AtomAnnotation.asRequired(Person)).build());

        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(asList(q1, q2, q3));
        checkIntersection(g, new float[][]{
                {0, 0, 1},
                {0, 0, 0},
                {0, 0, 0}
        });
    }

    @Test
    public void testUselessService() {
        QueryNode q1 = new QueryNode(empty, CQuery.from(new Triple(Y, name, author1)));
        QueryNode q2 = new QueryNode(empty, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.asRequired(Book))
                .annotate(Y, AtomAnnotation.of(Person)).build());
        QueryNode q3 = new QueryNode(empty, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.of(Book))
                .annotate(Y, AtomAnnotation.asRequired(Person)).build());
        MultiQueryNode mq = MultiQueryNode.builder().add(q2).add(q3).intersectInputs().build();

        for (List<PlanNode> ordering : asList(asList(q1, mq), asList(mq, q1))) {
            HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(ordering);
            PlanNode root = g.buildTree();
            assertTrue(root instanceof JoinNode);
            assertEquals(new HashSet<>(root.getChildren()), Sets.newHashSet(q1, q3));
        }
    }

    @Test
    public void testWithoutEquivalentSameQueryAndEp() {
        QueryNode q1 = new QueryNode(empty, CQuery.from(new Triple(X, name, Y)));
        QueryNode q2 = new QueryNode(empty, CQuery.from(new Triple(X, name, author1)));
        QueryNode q3 = new QueryNode(empty, CQuery.from(new Triple(X, name, Y)));

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : Collections2.permutations(asList(q1, q2, q3))) {
            HashSet<QueryNode> a = new HashSet<>(HeuristicPlanner.withoutEquivalents(permutation));
            assertTrue(a.equals(Sets.newHashSet(q1, q2)) || a.equals(Sets.newHashSet(q2, q3)));
        }
    }

    @Test
    public void testWithoutEquivalentIndirect() {
        List<EmptyEndpoint> eps = asList(new EmptyEndpoint(), new EmptyEndpoint(),
                                         new EmptyEndpoint());
        eps.get(0).addAlternative(eps.get(1));
        eps.get(1).addAlternative(eps.get(2));

        QueryNode q1 = new QueryNode(eps.get(0), CQuery.from(new Triple(X, name, Y)));
        QueryNode q2 = new QueryNode(eps.get(2), CQuery.from(new Triple(X, name, Y)));
        QueryNode q3 = new QueryNode(empty, CQuery.from(new Triple(X, name, author1)));
        QueryNode q4 = new QueryNode(empty, CQuery.with(new Triple(X, name, author1))
                .annotate(X, AtomAnnotation.of(Book)).build());


        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : Collections2.permutations(asList(q1, q2, q3, q4))) {
            HashSet<QueryNode> a = new HashSet<>(HeuristicPlanner.withoutEquivalents(permutation));
            assertTrue(a.equals(Sets.newHashSet(q1, q3, q4)) ||
                       a.equals(Sets.newHashSet(q2, q3, q4)));
        }
    }

    @Test
    public void testWithoutEquivalentSameMatched() {
        Triple t1 = new Triple(X, name, Y);
        Triple t2 = new Triple(X, mainName, Y);
        QueryNode q1, q2;
        q1 = new QueryNode(empty, CQuery.with(t1).annotate(t1, new MatchAnnotation(t1)).build());
        q2 = new QueryNode(empty, CQuery.with(t2).annotate(t2, new MatchAnnotation(t1)).build());

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : Collections2.permutations(asList(q1, q2))) {
            assertEquals(new HashSet<>(HeuristicPlanner.withoutEquivalents(permutation)),
                         Sets.newHashSet(q1, q2));
        }
    }

    @Test
    void testRemoveLeafAt() {
        float[][] intersection = new float[][] {
                {0,  1,  2,  3,  4,  5},
                {0,  0,  6,  7,  8,  9},
                {0,  0,  0, 10, 11, 12},
                {0,  0,  0,  0, 13, 14},
                {0,  0,  0,  0,  0, 15},
                {0,  0,  0,  0,  0,  0},
        };
        int size = 6;
        assertEquals(size, intersection.length);
        List<QueryNode> nodes = new ArrayList<>();
        for (int i = 0; i < size; i++)
            nodes.add(new QueryNode(new EmptyEndpoint(), CQuery.from(new Triple(X, knows, Y))));

        for (int removeIdx = 0; removeIdx < size; removeIdx++) {
            float[][] copy = new float[size][size];
            for (int i = 0; i < size; i++)
                System.arraycopy(intersection[i], 0, copy[i], 0, size);
            HeuristicPlanner.JoinGraph g;
            g = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes), copy);

            g.removeLeafAt(nodes.get(removeIdx), removeIdx);
            for (int i = 0; i < size; i++) {
                if (i == removeIdx) continue;
                for (int j = i; j < size; j++) {
                    if (j == removeIdx) continue;
                    float expected = intersection[i][j];
                    int i2 = i < removeIdx ? i : i-1;
                    int j2 = j < removeIdx ? j : j-1;
                    assertEquals(g.getIntersection()[i2][j2], expected);
                }
            }
        }
    }
}