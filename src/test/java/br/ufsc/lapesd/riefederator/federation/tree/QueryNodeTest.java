package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Capability;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Sets;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class QueryNodeTest implements TestContext {
    public static final @Nonnull StdLit i23 = StdLit.fromUnescaped("23", new StdURI(XSD.xint.getURI()));
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    private static final @Nonnull Atom atom1 = Molecule.builder("Atom1").buildAtom();
    private static final @Nonnull Atom atom2 = Molecule.builder("Atom2").buildAtom();
    private static final @Nonnull Atom atom3 = Molecule.builder("Atom3").buildAtom();

    @Test
    public void testNoVars() {
        CQuery query = CQuery.from(new Triple(Alice, knows, Bob));
        QueryNode node = new QueryNode(empty, query);
        assertSame(node.getEndpoint(), empty);
        assertSame(node.getQuery(), query);
        assertEquals(node.getResultVars(), emptySet());
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarsInTriple() {
        QueryNode node = new QueryNode(empty, CQuery.from(new Triple(Alice, knows, x)));
        assertEquals(node.getResultVars(), singleton("x"));
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarsInConjunctive() {
        CQuery query = CQuery.from(asList(new Triple(Alice, knows, x),
                                          new Triple(x, knows, y)));
        QueryNode node = new QueryNode(empty, query);
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertFalse(node.isProjecting());
        assertFalse(node.toString().startsWith("π"));
    }


    @Test
    public void testVarsInProjection() {
        CQuery query = CQuery.from(asList(new Triple(Alice, knows, x),
                                          new Triple(x, knows, y)));
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        assertEquals(node.getResultVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[x]("));
    }

    @Test
    public void testCreateBoundNoOp() {
        CQuery query = CQuery.with(asList(new Triple(Alice, knows, x),
                                          new Triple(x, knows, Bob))).distinct(true).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build("y", Charlie));

        assertEquals(bound.getQuery(), query);
        Modifier modifier = ModifierUtils.getFirst(Capability.DISTINCT,
                                                   bound.getQuery().getModifiers());
        assertNotNull(modifier);
        assertTrue(modifier.isRequired());
    }

    @Test
    public void testCreateBoundBindingOneVar() {
        CQuery query = CQuery.with(asList(new Triple(x, knows, Alice),
                                          new Triple(y, knows, x))).ask(false).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build("x", Bob));

        assertEquals(bound.getQuery(),
                     CQuery.with(asList(new Triple(Bob, knows, Alice),
                                        new Triple(y, knows, Bob))
                                ).ask(false).build());
        Modifier modifier = ModifierUtils.getFirst(Capability.ASK, bound.getQuery().getModifiers());
        assertTrue(modifier instanceof Ask);
        assertFalse(modifier.isRequired());
    }

    @Test
    public void testCreateBoundBindingOnlyVar() {
        CQuery query = CQuery.from(asList(new Triple(x, knows, Alice),
                                          new Triple(x, age, i23)));
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build("x", Bob));

        CQuery expected = CQuery.from(asList(new Triple(Bob, knows, Alice),
                                             new Triple(Bob, age,   i23)));
        assertEquals(bound.getQuery(), expected);
        assertTrue(bound.getQuery().getModifiers().isEmpty());
    }

    @Test
    public void testBindingRemovesProjectedVar() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        QueryNode bound = node.createBound(MapSolution.build(x, Alice));

        CQuery actual = bound.getQuery();
        assertEquals(actual, CQuery.from(new Triple(Alice, knows, y)));

        assertEquals(bound.getResultVars(), emptySet());
        assertTrue(bound.isProjecting());
    }

    @Test
    public void testBindingRemovesResultVar() {
        CQuery query = CQuery.from(new Triple(x, knows, y));
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(y, Bob));

        assertEquals(bound.getQuery(), CQuery.from(new Triple(x, knows, Bob)));
        assertEquals(bound.getResultVars(), singleton("x"));
        assertFalse(bound.hasInputs());
        assertFalse(bound.isProjecting());
    }

    @Test
    public void testBindingNoChangePreservesAnnotations() {
        CQuery query = CQuery.with(new Triple(Alice, knows, y))
                .annotate(Alice, AtomAnnotation.of(atom1))
                .annotate(y, AtomAnnotation.asRequired(atom2)).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(x, Bob));

        assertEquals(bound.getResultVars(), singleton("y"));
        assertTrue(bound.hasInputs());
        assertEquals(bound.getInputVars(), singleton("y"));
        assertTrue(bound.getQuery().hasTermAnnotations());
        assertFalse(bound.getQuery().hasTripleAnnotations());
        assertEquals(bound.getQuery(), query);
    }

    @Test
    public void testBindingPreservesAnnotations() {
        CQuery query = CQuery.with(new Triple(x, knows, y), new Triple(Alice, knows, x))
                .annotate(x, AtomAnnotation.asRequired(atom1))
                .annotate(y, AtomAnnotation.asRequired(atom2))
                .annotate(Alice, AtomAnnotation.of(atom3))
                .build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(x, Bob));

        assertEquals(bound.getResultVars(), singleton("y"));
        assertTrue(bound.hasInputs());
        assertEquals(bound.getInputVars(), singleton("y"));

        CQuery expected = CQuery.with(new Triple(Bob, knows, y), new Triple(Alice, knows, Bob))
                .annotate(Bob, AtomAnnotation.asRequired(atom1))
                .annotate(y, AtomAnnotation.asRequired(atom2))
                .annotate(Alice, AtomAnnotation.of(atom3))
                .build();
        assertEquals(bound.getQuery(), expected);
    }
}