package br.ufsc.lapesd.freqel.algebra;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(groups = {"fast"})
public class UnionOpTest implements TestContext {
    public static final @Nonnull EmptyEndpoint empty = new EmptyEndpoint();

    public EndpointQueryOp aliceKnowsX, bobKnowsX, xKnowsY;

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new EndpointQueryOp(empty, CQuery.from(new Triple(Alice, knows, x)));
        bobKnowsX = new EndpointQueryOp(empty, CQuery.from(new Triple(Bob, knows, x)));
        xKnowsY = new EndpointQueryOp(empty, CQuery.from(new Triple(x, knows, y)));
    }

    @Test
    public void testSimple() {
        Op node = UnionOp.builder().add(aliceKnowsX).add(bobKnowsX).build();
        assertFalse(node.isProjected());
        assertEquals(node.getResultVars(), singleton("x"));
        assertEquals(node.getChildren(), asList(aliceKnowsX, bobKnowsX));
    }

    @Test
    public void testAllVars() {
        Op node = UnionOp.builder().add(aliceKnowsX).add(xKnowsY).build();
        assertFalse(node.isProjected());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getChildren(), asList(aliceKnowsX, xKnowsY));

        // order does not matter
        node = UnionOp.builder().add(xKnowsY).add(aliceKnowsX).build();
        assertFalse(node.isProjected());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getChildren(), asList(xKnowsY, aliceKnowsX));
    }
}