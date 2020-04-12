package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class JoinNodeTest implements TestContext {
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    public QueryNode aliceKnowsX, xKnowsY, yKnown, xKnowsYInput, yInputKnowsAlice;
    private static final Atom Person = Molecule.builder("Person").buildAtom();
    private static final Atom KnownPerson = Molecule.builder("KnownPerson").buildAtom();

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new QueryNode(empty, CQuery.from(new Triple(Alice, knows, x)));
        xKnowsY = new QueryNode(empty, CQuery.from(new Triple(x, knows, y)));
        xKnowsYInput = new QueryNode(empty, CQuery.with(new Triple(x, knows, y))
                .annotate(x, AtomAnnotation.of(Person))
                .annotate(y, AtomInputAnnotation.asRequired(KnownPerson, "knownPerson").get())
                .build());
        yKnown = new QueryNode(empty, CQuery.from(new Triple(x, knows, y)), singleton("y"));
        yInputKnowsAlice = new QueryNode(empty, createQuery(
                y, AtomInputAnnotation.asRequired(Person, "person").get(), knows, Alice
        ));
    }

    @Test
    public void testSimpleJoin() {
        JoinNode node = JoinNode.builder(aliceKnowsX, xKnowsY).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        // order shouldn't change the result
        node = JoinNode.builder(xKnowsY, aliceKnowsX).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @Test
    public void testManuallySetResultVarsNotProjecting() {
        JoinNode node = JoinNode.builder(aliceKnowsX, xKnowsY)
                .setResultVars(newHashSet("x", "y")).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
        assertFalse(node.toString().contains("π"));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        // order shouldn't change the result
        node = JoinNode.builder(xKnowsY, aliceKnowsX)
                .setResultVars(newHashSet("x", "y")).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @Test
    public void testProjectJoinVarOut() {
        JoinNode node = JoinNode.builder(aliceKnowsX, xKnowsY).addResultVar("y").build();
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[y]("));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        node = JoinNode.builder(xKnowsY, aliceKnowsX).addResultVar("y").build();
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[y]("));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @SuppressWarnings("SuspiciousNameCombination")
    @Test
    public void testUseVarProjectedOut() {
        // Since x is projected ou on yKnown, it can't be used to join
        expectThrows(IllegalArgumentException.class,
                () -> JoinNode.builder(yKnown, aliceKnowsX).build());
        expectThrows(IllegalArgumentException.class,
                () -> JoinNode.builder(yKnown, aliceKnowsX).addResultVar("x").build());

        //order does not change result
        expectThrows(IllegalArgumentException.class,
                () -> JoinNode.builder(aliceKnowsX, yKnown).build());
        expectThrows(IllegalArgumentException.class,
                () -> JoinNode.builder(aliceKnowsX, yKnown).addResultVar("x").build());
    }

    @Test
    public void testReplaceChildWithInput() {
        JoinNode join = JoinNode.builder(aliceKnowsX, xKnowsYInput).build();

        Map<PlanNode, PlanNode> replacement = new HashMap<>();
        replacement.put(xKnowsYInput, xKnowsY);
        JoinNode replaced = join.replacingChildren(replacement);

        assertEquals(replaced.getResultVars(), join.getResultVars());
        assertEquals(replaced.getRequiredInputVars(), emptySet());
        assertFalse(replaced.hasInputs());
        assertEquals(replaced.getJoinVars(), join.getJoinVars());
    }

    @SuppressWarnings("SuspiciousNameCombination")
    @Test
    public void testJoinNodeResolvesInput() {
        JoinNode node = JoinNode.builder(xKnowsY, yInputKnowsAlice).build();

        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getStrictResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), emptySet());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        node = JoinNode.builder(yInputKnowsAlice, xKnowsY).build();
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getStrictResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), emptySet());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

    }
}