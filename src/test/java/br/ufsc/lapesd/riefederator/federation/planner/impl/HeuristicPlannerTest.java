package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.BitSet;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class HeuristicPlannerTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar U = new StdVar("u");
    public static final @Nonnull StdVar V = new StdVar("v");

    public static final @Nonnull EmptyEndpoint empty = new EmptyEndpoint();
    public QueryNode xKnowsY, aliceKnowsX, yKnowsBob;
    public QueryNode uKnowsV, aliceKnowsU, vKnowsBob;

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
        int[][] intersection = new int[][] {
                new int[] {0, 1},
                new int[] {0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        assertTrue(g.tryJoin());
        assertFalse(g.tryJoin());

        PlanNode root = g.getRoot();
        assertTrue(root instanceof JoinNode);
        assertFalse(root.isProjecting());
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(((JoinNode)root).getJoinVars(), singleton("x"));
    }

    @Test
    public void testJoinThreeHopPath() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        int[][] intersection = new int[][] {
                new int[] {0, 1, 0},
                new int[] {0, 0, 1},
                new int[] {0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        assertTrue(g.tryJoin());
        assertTrue(g.tryJoin());
        assertFalse(g.tryJoin());

        PlanNode root = g.getRoot();
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
        int[][] intersection = new int[][]{
                new int[]{0}
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        assertFalse(g.tryJoin());
        assertSame(g.getRoot(), leaves.get(0));
    }

    @Test
    public void testCreateTotalComponent() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY));
        int[][] intersection = new int[][] {
                new int[]{0, 1},
                new int[]{0, 0}
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
        int[][] intersection = new int[][] {
                new int[]{0, 1, 0},
                new int[]{0, 0, 1},
                new int[]{0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);
        for (int i = 0; i < leaves.size(); i++) {
            BitSet subset = new BitSet();
            subset.set(i);
            HeuristicPlanner.JoinGraph component = g.createComponent(subset);
            assertEquals(component.getLeaves(), singletonList(leaves.get(i)));
            assertEquals(component.getIntersection().length, 1);
            assertEquals(component.getIntersection()[0].length, 1);
            assertEquals(component.getIntersection()[0][0], 0);
        }
    }

    @Test
    public void testCreatePairComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob));
        int[][] intersection = {
                new int[] {0, 1, 0},
                new int[] {0, 0, 1},
                new int[] {0, 0, 0},
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
                assertEquals(component.getIntersection()[0], new int[]{0, 1}, "i="+i);
            else
                assertEquals(component.getIntersection()[0], new int[]{0, 0}, "i="+i);
            assertEquals(component.getIntersection()[1], new int[]{0, 0}, "i="+i);
        }
    }

    @Test
    public void testCreateDisjointComponents() {
        ArrayList<PlanNode> leaves = new ArrayList<>(asList(aliceKnowsX, xKnowsY, yKnowsBob,
                                                             aliceKnowsU, uKnowsV, vKnowsBob));
        int[][] intersection = {
                new int[] {0, 1, 0, 0, 0, 0},
                new int[] {0, 0, 1, 0, 0, 0},
                new int[] {0, 0, 0, 0, 0, 0},
                new int[] {0, 0, 0, 0, 1, 0},
                new int[] {0, 0, 0, 0, 0, 1},
                new int[] {0, 0, 0, 0, 0, 0},
        };
        HeuristicPlanner.JoinGraph g = new HeuristicPlanner.JoinGraph(leaves, intersection);

        BitSet subset = new BitSet();
        subset.set(0, 3);
        HeuristicPlanner.JoinGraph left = g.createComponent(subset);
        assertEquals(left.getLeaves(), asList(aliceKnowsX, xKnowsY, yKnowsBob));
        int[][] exComponent = {
                new int[] {0, 1, 0},
                new int[] {0, 0, 1},
                new int[] {0, 0, 0},
        };
        assertEquals(left.getIntersection().length, 3);
        for (int i = 0; i < 3; i++)
            assertEquals(left.getIntersection()[i], exComponent[i], "i="+i);

        subset.clear();
        subset.set(3, 6);
        HeuristicPlanner.JoinGraph right = g.createComponent(subset);
        assertEquals(right.getLeaves(), asList(aliceKnowsU, uKnowsV, vKnowsBob));
        assertEquals(right.getIntersection().length, 3);
        for (int i = 0; i < 3; i++)
            assertEquals(right.getIntersection()[i], exComponent[i], "i="+i);
    }

}