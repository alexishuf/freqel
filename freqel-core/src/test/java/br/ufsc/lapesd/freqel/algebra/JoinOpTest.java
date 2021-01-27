package br.ufsc.lapesd.freqel.algebra;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParserTest;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.freqel.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.freqel.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class JoinOpTest implements TestContext {
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    public EndpointQueryOp aliceKnowsX, xKnowsY, yKnown, xKnowsYInput, yInputKnowsAlice;
    private static final Atom Person = Molecule.builder("Person").buildAtom();
    private static final Atom KnownPerson = Molecule.builder("KnownPerson").buildAtom();

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new EndpointQueryOp(empty, CQuery.from(new Triple(Alice, knows, x)));
        xKnowsY = new EndpointQueryOp(empty, CQuery.from(new Triple(x, knows, y)));
        xKnowsYInput = new EndpointQueryOp(empty, createQuery(x, AtomAnnotation.of(Person),
                knows, y, AtomInputAnnotation.asRequired(KnownPerson, "knownPerson").get()));
        yKnown = new EndpointQueryOp(empty, createQuery(x, knows, y, Projection.of("y")));
        yInputKnowsAlice = new EndpointQueryOp(empty, createQuery(
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
        node.modifiers().add(Projection.of("x", "y"));
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertTrue(node.toString().contains("π"));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        // order shouldn't change the result
        node = JoinOp.create(xKnowsY, aliceKnowsX);
        node.modifiers().add(Projection.of("x", "y"));
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());
    }

    @Test
    public void testProjectJoinVarOut() {
        JoinOp node = JoinOp.create(aliceKnowsX, xKnowsY);
        node.modifiers().add(Projection.of("y"));
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjected());
        assertTrue(node.toString().startsWith("π[y]("));
        assertEquals(node.getInputVars(), emptySet());
        assertFalse(node.hasInputs());

        node = JoinOp.create(xKnowsY, aliceKnowsX);
        node.modifiers().add(Projection.of("y"));
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

    @Test
    public void testDeepCopyPreservesUniverseSets() {
        QueryOp left = new QueryOp(createQuery(
                Alice, knows, x, Alice, age, u, SPARQLFilter.build("?u < ?v")));
        QueryOp right = new QueryOp(createQuery(x, age, v, SPARQLFilter.build("?v > 23")));
        IndexSet<Triple> triplesUniverse = FullIndexSet.newIndexSet(
                new Triple(Alice, knows, x),
                new Triple(Alice, age, u),
                new Triple(x, age, v)
        );
        IndexSet<String> varsUniverse = FullIndexSet.newIndexSet("x", "u", "v", "y");
        left .offerVarsUniverse(varsUniverse); left .offerTriplesUniverse(triplesUniverse);
        right.offerVarsUniverse(varsUniverse); right.offerTriplesUniverse(triplesUniverse);
        SPARQLParserTest.assertUniverses(left);
        SPARQLParserTest.assertUniverses(right);

        JoinOp join = JoinOp.create(left, right);
        join.modifiers().add(Projection.of("x", "y"));
        join.offerTriplesUniverse(triplesUniverse);
        join.offerVarsUniverse(varsUniverse);
        SPARQLParserTest.assertUniverses(join);

        Op copy = TreeUtils.deepCopy(join);
        assertNotSame(copy, join);
        assertEquals(copy, join);
        SPARQLParserTest.assertUniverses(copy);

        assertSame(((IndexSubset<Triple>)copy.getMatchedTriples()).getParent(), triplesUniverse);
        assertSame(((IndexSubset<String>)copy.getAllVars()).getParent(), varsUniverse);
    }
}