package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class JoinNodeTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bon");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI manages = new StdURI("http://example.org/manages");
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    public QueryNode aliceKnowsX, xKnowsY, yKnown, xKnowsYInput;
    private static final Atom Person = Molecule.builder("Person").buildAtom();
    private static final Atom KnownPerson = Molecule.builder("KnownPerson").buildAtom();

    @BeforeMethod
    public void setUp() {
        aliceKnowsX = new QueryNode(empty, CQuery.from(new Triple(ALICE, knows, X)));
        xKnowsY = new QueryNode(empty, CQuery.from(new Triple(X, knows, Y)));
        xKnowsYInput = new QueryNode(empty, CQuery.with(new Triple(X, knows, Y))
                .annotate(X, AtomAnnotation.of(Person))
                .annotate(Y, AtomAnnotation.asRequired(KnownPerson))
                .build());
        yKnown = new QueryNode(empty, CQuery.from(new Triple(X, knows, Y)), singleton("y"));
    }

    @Test
    public void testSimpleJoin() {
        JoinNode node = JoinNode.builder(aliceKnowsX, xKnowsY).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());

        // order shouldn't change the result
        node = JoinNode.builder(xKnowsY, aliceKnowsX).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
    }

    @Test
    public void testManuallySetResultVarsNotProjecting() {
        JoinNode node = JoinNode.builder(aliceKnowsX, xKnowsY)
                .setResultVarsNoProjection(newHashSet("x", "y")).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
        assertFalse(node.toString().contains("π"));

        // order shouldn't change the result
        node = JoinNode.builder(xKnowsY, aliceKnowsX)
                .setResultVarsNoProjection(newHashSet("x", "y")).build();
        assertEquals(node.getResultVars(), newHashSet("x", "y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertFalse(node.isProjecting());
    }

    @Test
    public void testProjectJoinVarOut() {
        JoinNode node = JoinNode.builder(aliceKnowsX, xKnowsY).addResultVar("y").build();
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[y]("));

        node = JoinNode.builder(xKnowsY, aliceKnowsX).addResultVar("y").build();
        assertEquals(node.getResultVars(), newHashSet("y"));
        assertEquals(node.getJoinVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[y]("));
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

    @SuppressWarnings("SuspiciousNameCombination")
    @Test
    public void testReplaceChildWithInput() {
        JoinNode join = JoinNode.builder(yKnown, xKnowsYInput).build();

        Map<PlanNode, PlanNode> replacement = new HashMap<>();
        replacement.put(xKnowsYInput, xKnowsY);
        JoinNode replaced = join.replacingChildren(replacement);

        assertEquals(replaced.getResultVars(), join.getResultVars());
        assertEquals(replaced.getInputVars(), emptySet());
        assertFalse(replaced.hasInputs());
        assertEquals(replaced.getJoinVars(), join.getJoinVars());
    }
}