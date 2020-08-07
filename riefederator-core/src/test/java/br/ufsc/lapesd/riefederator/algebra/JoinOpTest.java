package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class JoinOpTest implements TestContext {
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    public QueryOp aliceKnowsX, xKnowsY, yKnown, xKnowsYInput, yInputKnowsAlice;
    private static final Atom Person = Molecule.builder("Person").buildAtom();
    private static final Atom KnownPerson = Molecule.builder("KnownPerson").buildAtom();

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new QueryOp(empty, CQuery.from(new Triple(Alice, knows, x)));
        xKnowsY = new QueryOp(empty, CQuery.from(new Triple(x, knows, y)));
        xKnowsYInput = new QueryOp(empty, createQuery(x, AtomAnnotation.of(Person),
                knows, y, AtomInputAnnotation.asRequired(KnownPerson, "knownPerson").get()));
        yKnown = new QueryOp(empty, createQuery(x, knows, y, Projection.advised("y")));
        yInputKnowsAlice = new QueryOp(empty, createQuery(
                y, AtomInputAnnotation.asRequired(Person, "person").get(), knows, Alice
        ));
    }

    @Test
    public void testSimpleJoin() {
        JoinOp node = JoinOp.create(aliceKnowsX, xKnowsY);
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjected());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        // order shouldn't change the result
        node = JoinOp.create(xKnowsY, aliceKnowsX);
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjected());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @Test
    public void testManuallySetResultVarsNotProjecting() {
        JoinOp node = JoinOp.create(aliceKnowsX, xKnowsY);
        node.modifiers().add(Projection.advised("x", "y"));
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertTrue(node.toString().contains("π"));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        // order shouldn't change the result
        node = JoinOp.create(xKnowsY, aliceKnowsX);
        node.modifiers().add(Projection.advised("x", "y"));
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @Test
    public void testProjectJoinVarOut() {
        JoinOp node = JoinOp.create(aliceKnowsX, xKnowsY);
        node.modifiers().add(Projection.advised("y"));
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertTrue(node.toString().startsWith("π[y]("));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        node = JoinOp.create(xKnowsY, aliceKnowsX);
        node.modifiers().add(Projection.advised("y"));
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertTrue(node.toString().startsWith("π[y]("));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @SuppressWarnings("SuspiciousNameCombination")
    @Test
    public void testUseVarProjectedOut() {
        // Since x is projected ou on yKnown, it can't be used to join
        expectThrows(IllegalArgumentException.class,
                () -> JoinOp.create(yKnown, aliceKnowsX));

        //order does not change result
        expectThrows(IllegalArgumentException.class,
                () -> JoinOp.create(aliceKnowsX, yKnown));
    }

    @Test
    public void testReplaceChildWithInput() {
        JoinOp join = JoinOp.create(aliceKnowsX, xKnowsYInput);

        JoinOp replaced = (JoinOp)TreeUtils.replaceNodes(join, null, op -> {
            if (op.equals(xKnowsYInput))
                return xKnowsY;
            return op;
        });

        assertEquals(replaced.getResultVars(), join.getResultVars());
        assertEquals(replaced.getRequiredInputVars(), emptySet());
        assertFalse(replaced.hasInputs());
        assertEquals(replaced.getJoinVars(), join.getJoinVars());
    }

    @SuppressWarnings("SuspiciousNameCombination")
    @Test
    public void testJoinNodeResolvesInput() {
        JoinOp node = JoinOp.create(xKnowsY, yInputKnowsAlice);

        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), emptySet());
        assertEquals(node.getInputVars(), emptySet());
        assertEquals(node.getStrictResultVars(), Sets.newHashSet("x", "y"));
        assertFalse(node.hasInputs());

        node = JoinOp.create(yInputKnowsAlice, xKnowsY);
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), emptySet());
        assertEquals(node.getInputVars(), emptySet());
        assertEquals(node.getStrictResultVars(), Sets.newHashSet("x", "y"));
        assertFalse(node.hasInputs());

    }
}