package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class MultiQueryNodeTest implements TestContext {
    public static final @Nonnull EmptyEndpoint empty = new EmptyEndpoint();

    public QueryNode aliceKnowsX, bobKnowsX, xKnowsY;

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new QueryNode(empty, CQuery.from(new Triple(Alice, knows, x)));
        bobKnowsX = new QueryNode(empty, CQuery.from(new Triple(Bob, knows, x)));
        xKnowsY = new QueryNode(empty, CQuery.from(new Triple(x, knows, y)));
    }

    @Test
    public void testSimple() {
        MultiQueryNode node = MultiQueryNode.builder().add(aliceKnowsX).add(bobKnowsX).build();
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), singleton("x"));
        assertEquals(node.getChildren(), asList(aliceKnowsX, bobKnowsX));
    }

    @Test
    public void testAllVars() {
        MultiQueryNode node = MultiQueryNode.builder().add(aliceKnowsX).add(xKnowsY)
                                                      .allVars().build();
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getChildren(), asList(aliceKnowsX, xKnowsY));

        // order does not matter
        node = MultiQueryNode.builder().add(xKnowsY).add(aliceKnowsX)
                .allVars().build();
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getChildren(), asList(xKnowsY, aliceKnowsX));
    }

    @Test
    public void testIntersect() {
        MultiQueryNode node = MultiQueryNode.builder().add(aliceKnowsX).add(bobKnowsX).add(xKnowsY)
                                            .intersect().build();
        assertTrue(node.isProjecting());
        assertEquals(node.getResultVars(), singleton("x"));
        assertEquals(node.getChildren(), asList(aliceKnowsX, bobKnowsX, xKnowsY));

        node = MultiQueryNode.builder().add(xKnowsY).add(bobKnowsX).add(aliceKnowsX)
                .intersect().build();
        assertTrue(node.isProjecting());
        assertEquals(node.getResultVars(), singleton("x"));
        assertEquals(node.getChildren(), asList(xKnowsY, bobKnowsX, aliceKnowsX));

        node = MultiQueryNode.builder().add(bobKnowsX).add(xKnowsY).add(aliceKnowsX)
                .intersect().build();
        assertTrue(node.isProjecting());
        assertEquals(node.getResultVars(), singleton("x"));
        assertEquals(node.getChildren(), asList(bobKnowsX, xKnowsY, aliceKnowsX));
    }
}