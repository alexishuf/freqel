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
import java.util.*;

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
    public static final @Nonnull StdVar W = new StdVar("w");
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

    @Test
    public void testTryJoinSimple() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY));
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, new float[]{1});
        assertTrue(g.tryJoin());
        assertFalse(g.tryJoin());

        PlanNode root = g.get(0);
        assertTrue(root instanceof JoinNode);
        assertFalse(root.isProjecting());
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(((JoinNode)root).getJoinVars(), singleton("x"));
    }

    @Test
    public void testJoinThreeHopPath() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, new float[]{
                //xKnowsY yKnowsBob
                  1,      0, //aliceKnowsX
                          1  // xKnowsY
        });
        assertTrue(g.tryJoin());
        assertTrue(g.tryJoin());
        assertFalse(g.tryJoin());

        PlanNode root = g.get(0);
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
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, new float[0]);

        assertFalse(g.tryJoin());
        assertSame(g.get(0), leaves.get(0));
    }

    @Test
    public void testCreateTotalComponent() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY));
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, new float[]{1});
        BitSet subset = new BitSet();
        subset.set(0, 2);
        HeuristicPlanner.JoinGraph component = g.createComponent(subset);

        assertEquals(component.getNodes(), leaves);
        assertEquals(component.getWeights(), new float[]{1});
    }

    @Test
    public void testCreateUnitComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        float[] intersection = new float[] {
                //xKnowsY, yKnowsBob
                  1,       0,        //aliceKnowsX
                           1         //xKnowsY
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        for (int i = 0; i < leaves.size(); i++) {
            BitSet subset = new BitSet();
            subset.set(i);
            HeuristicPlanner.JoinGraph component = g.createComponent(subset);
            assertEquals(component.getNodes(), singletonList(leaves.get(i)));
            assertEquals(component.getWeights().length, 0);
        }
    }

    @Test
    public void testCreatePairComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        float[] intersection = {
                //xKnowsY, yKnowsBob
                  1,       0,        //aliceKnowsX
                           1,        //xKnowsY
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        for (int i = 0; i < leaves.size(); i++) {
            BitSet subset = new BitSet();
            subset.set(i);
            subset.set((i+1) % leaves.size());
            HeuristicPlanner.JoinGraph component = g.createComponent(subset);

            if (i < leaves.size()-1)
                assertEquals(component.getNodes(), asList(leaves.get(i), leaves.get(i+1)));
            else
                assertEquals(component.getNodes(), asList(leaves.get(0), leaves.get(i)));
            assertEquals(component.getWeights(), new float[]{i < leaves.size()-1 ? 1 : 0});
        }
    }

    @Test
    public void testCreateDisjointComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob,
                                                             aliceKnowsU, uKnowsV, vKnowsBob));
        float[] intersection = {
                //xKnowsY, yKnowsBob, aliceKnowsU, uKnowsV, vKnowsBob
                  1,       0,         0,           0,       0,       //aliceKnowsX
                           1,         0,           0,       0,       //xKnowsY
                                      0,           0,       0,       //yKnowsBob
                                                   1,       0,       //aliceKnowsU
                                                            1        //uKnowsV
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        BitSet subset = new BitSet();
        subset.set(0, 3);
        HeuristicPlanner.JoinGraph left = g.createComponent(subset);
        assertEquals(left.getNodes(), asList(aliceKnowsX, xKnowsY, yKnowsBob));
        assertEquals(left.getWeights(), new float[]{
                //xKnowsY, yKnowsBob
                  1,       0,      //aliceKnowsX
                           1       //xKnowsY

        });

        subset.clear();
        subset.set(3, 6);
        HeuristicPlanner.JoinGraph right = g.createComponent(subset);
        assertEquals(right.getNodes(), asList(aliceKnowsU, uKnowsV, vKnowsBob));
        assertEquals(right.getWeights(), new float[]{
                //uKnowsV, vKnowsBob
                  1,       0,       //aliceKnowsU
                           1        //uKnowsV
        });
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

        assertEquals(g.getWeights(), new float[] {
                //xNameY, xLikesZ
                  1,      1,     //xKnowsBob
                          0      //xNameY
        });
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

        assertEquals(g.getWeights(), new float[] {
                //q2 q3
                  1, 0, //q1
                     1, //q2
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
        assertEquals(g.getWeights(), new float[]{
                //q2, q3
                  0,  1, //q1
                      0  //q2
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
        float[] weights = new float[] {
                1,  2,  3,  4,  5,
                    6,  7,  8,  9,
                       10, 11, 12,
                           13, 14,
                               15
        };
        int nodeCount = 6;
        List<QueryNode> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++)
            nodes.add(new QueryNode(new EmptyEndpoint(), CQuery.from(new Triple(X, knows, Y))));
        HeuristicPlanner.JoinGraph original;
        original = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes), weights);

        for (int removeIdx = 0; removeIdx < nodeCount; removeIdx++) {
            HeuristicPlanner.JoinGraph g;
            g = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes),
                                               Arrays.copyOf(weights, weights.length));

            g.removeLeafAt(nodes.get(removeIdx), removeIdx);
            for (int i = 0; i < nodeCount; i++) {
                if (i == removeIdx) continue;
                for (int j = i; j < nodeCount; j++) {
                    if (j == removeIdx || i == j) continue;
                    float expected = original.getWeight(i, j);
                    int i2 = i < removeIdx ? i : i-1;
                    int j2 = j < removeIdx ? j : j-1;
                    assertEquals(g.getWeight(i2, j2), expected);
                }
            }
        }
    }

    @Test
    void testReplaceWithJoinInPath() {
        List<QueryNode> nodes = asList(
                new QueryNode(empty, CQuery.from(new Triple(X, knows, Y))),
                new QueryNode(empty, CQuery.from(new Triple(Y, knows, Z))),
                new QueryNode(empty, CQuery.from(new Triple(Z, knows, W))),
                new QueryNode(empty, CQuery.from(new Triple(W, knows, U))),
                new QueryNode(empty, CQuery.from(new Triple(U, knows, V))));

        HeuristicPlanner.JoinGraph g0 = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes));
        assertEquals(g0.getWeights(), new float[] {
                1, 0, 0, 0, // X knows Y
                   1, 0, 0, // Y knows Z
                      1, 0, // Z knows W
                         1  // W knows U
        });

        for (int lefIdx = 0; lefIdx < nodes.size() - 1; lefIdx++) {
            HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes));
            g.replaceWithJoin(lefIdx, lefIdx+1);
            assertEquals(g.size(), nodes.size()-1);
            List<PlanNode> leaves = g.getNodes();
            for (int i = 0; i < leaves.size() - 1; i++) {
                for (int j = 0; j < i-1; j++)
                    assertEquals(g.getWeight(i, j), 0.0f);
                assertEquals(g.getWeight(i, i+1), 1.0f);
                for (int j = i+2; j < leaves.size()-1; j++)
                    assertEquals(g.getWeight(i, j), 0.0f);
            }
        }
    }

    @Test
    void testReplaceWithJoinInNonPath() {
        List<QueryNode> nodes = asList(
                new QueryNode(empty, CQuery.from(new Triple(X, knows, Y))),
                new QueryNode(empty, CQuery.from(new Triple(X, likes, Y))),
                new QueryNode(empty, CQuery.from(new Triple(Y, knows, U))),
                new QueryNode(empty, CQuery.from(new Triple(U, knows, V))),
                new QueryNode(empty, CQuery.from(new Triple(U, likes, V))),
                new QueryNode(empty, CQuery.from(new Triple(V, knows, X))));

        HeuristicPlanner.JoinGraph g0 = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes));
        assertEquals(g0.getWeights(), new float[]{
                2,   1,   0, 0,   1, // X knows Y
                     1,   0, 0,   1, // X likes Y
                          1, 1,   0, // Y knows U
                             2,   1, // U knows V
                                  1 // U likes V
        });

        HeuristicPlanner.JoinGraph g1 = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes));
        g1.replaceWithJoin(0, 1);
        assertEquals(g1.getWeights(), new float[] {
                1,   0, 0,   1, // X knows Y ⋈ X likes Y
                     1, 1,   0, // Y knows U
                        2,   1, // U knows V
                             1 // U likes V
        });

        HeuristicPlanner.JoinGraph g2 = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes));
        g2.replaceWithJoin(2, 3);
        assertEquals(g2.getWeights(), new float[] {
                2,   1,   0,   1, // X knows Y
                     1,   0,   1, // X likes Y
                          2,   1, // Y knows U ⋈ U knows V
                               1  // U likes V
        });

        HeuristicPlanner.JoinGraph g3 = new HeuristicPlanner.JoinGraph(new ArrayList<>(nodes));
        g3.replaceWithJoin(0, 5);
        assertEquals(g3.getWeights(), new float[] {
                2,   1,   1, 1, // X knows Y ⋈ V knows X
                     1,   0, 0, // X likes Y
                          1, 1, // Y knows U
                             2  // U knows V
        });
    }
}