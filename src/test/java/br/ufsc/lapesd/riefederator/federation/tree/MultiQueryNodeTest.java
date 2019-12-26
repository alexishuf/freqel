package br.ufsc.lapesd.riefederator.federation.tree;

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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class MultiQueryNodeTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bon");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI manages = new StdURI("http://example.org/manages");
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull EmptyEndpoint empty = new EmptyEndpoint();

    public QueryNode aliceKnowsX, bobKnowsX, xKnowsY;

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new QueryNode(empty, CQuery.from(new Triple(ALICE, knows, X)));
        bobKnowsX = new QueryNode(empty, CQuery.from(new Triple(BOB, knows, X)));
        xKnowsY = new QueryNode(empty, CQuery.from(new Triple(X, knows, Y)));
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