package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Capability;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class QueryNodeTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI CHARLIE = new StdURI("http://example.org/Charlie");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI age = new StdURI(FOAF.age.getURI());
    public static final @Nonnull StdLit i23 = StdLit.fromUnescaped("23", new StdURI(XSD.xint.getURI()));
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    private static final EmptyEndpoint empty = new EmptyEndpoint();

    private static final @Nonnull Atom atom1 = Molecule.builder("Atom1").buildAtom();
    private static final @Nonnull Atom atom2 = Molecule.builder("Atom2").buildAtom();
    private static final @Nonnull Atom atom3 = Molecule.builder("Atom3").buildAtom();

    @Test
    public void testNoVars() {
        CQuery query = CQuery.from(new Triple(ALICE, knows, BOB));
        QueryNode node = new QueryNode(empty, query);
        assertSame(node.getEndpoint(), empty);
        assertSame(node.getQuery(), query);
        assertEquals(node.getResultVars(), emptySet());
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarsInTriple() {
        QueryNode node = new QueryNode(empty, CQuery.from(new Triple(ALICE, knows, X)));
        assertEquals(node.getResultVars(), singleton("x"));
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarsInConjunctive() {
        CQuery query = CQuery.from(asList(new Triple(ALICE, knows, X),
                                          new Triple(X, knows, Y)));
        QueryNode node = new QueryNode(empty, query);
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertFalse(node.isProjecting());
        assertFalse(node.toString().startsWith("π"));
    }


    @Test
    public void testVarsInProjection() {
        CQuery query = CQuery.from(asList(new Triple(ALICE, knows, X),
                                          new Triple(X, knows, Y)));
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        assertEquals(node.getResultVars(), singleton("x"));
        assertTrue(node.isProjecting());
        assertTrue(node.toString().startsWith("π[x]("));
    }

    @Test
    public void testCreateBoundNoOp() {
        CQuery query = CQuery.with(asList(new Triple(ALICE, knows, X),
                                          new Triple(X, knows, BOB))).distinct(true).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build("y", CHARLIE));

        assertEquals(bound.getQuery(), query);
        Modifier modifier = ModifierUtils.getFirst(Capability.DISTINCT,
                                                   bound.getQuery().getModifiers());
        assertNotNull(modifier);
        assertTrue(modifier.isRequired());
    }

    @Test
    public void testCreateBoundBindingOneVar() {
        CQuery query = CQuery.with(asList(new Triple(X, knows, ALICE),
                                          new Triple(Y, knows, X))).ask(false).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build("x", BOB));

        assertEquals(bound.getQuery(),
                     CQuery.with(asList(new Triple(BOB, knows, ALICE),
                                        new Triple(Y, knows, BOB))
                                ).ask(false).build());
        Modifier modifier = ModifierUtils.getFirst(Capability.ASK, bound.getQuery().getModifiers());
        assertTrue(modifier instanceof Ask);
        assertFalse(modifier.isRequired());
    }

    @Test
    public void testCreateBoundBindingOnlyVar() {
        CQuery query = CQuery.from(asList(new Triple(X, knows, ALICE),
                                          new Triple(X, age, i23)));
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build("x", BOB));

        CQuery expected = CQuery.from(asList(new Triple(BOB, knows, ALICE),
                                             new Triple(BOB, age,   i23)));
        assertEquals(bound.getQuery(), expected);
        assertTrue(bound.getQuery().getModifiers().isEmpty());
    }

    @Test
    public void testBindingRemovesProjectedVar() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        QueryNode bound = node.createBound(MapSolution.build(X, ALICE));

        CQuery actual = bound.getQuery();
        assertEquals(actual, CQuery.from(new Triple(ALICE, knows, Y)));

        assertEquals(bound.getResultVars(), emptySet());
        assertTrue(bound.isProjecting());
    }

    @Test
    public void testBindingRemovesResultVar() {
        CQuery query = CQuery.from(new Triple(X, knows, Y));
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(Y, BOB));

        assertEquals(bound.getQuery(), CQuery.from(new Triple(X, knows, BOB)));
        assertEquals(bound.getResultVars(), singleton("x"));
        assertFalse(bound.hasInputs());
        assertFalse(bound.isProjecting());
    }

    @Test
    public void testBindingNoChangePreservesAnnotations() {
        CQuery query = CQuery.with(new Triple(ALICE, knows, Y))
                .annotate(ALICE, AtomAnnotation.of(atom1))
                .annotate(Y, AtomAnnotation.asRequired(atom2)).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(X, BOB));

        assertEquals(bound.getResultVars(), singleton("y"));
        assertTrue(bound.hasInputs());
        assertEquals(bound.getInputVars(), singleton("y"));
        assertTrue(bound.getQuery().hasTermAnnotations());
        assertFalse(bound.getQuery().hasTripleAnnotations());
        assertEquals(bound.getQuery(), query);
    }

    @Test
    public void testBindingPreservesAnnotations() {
        CQuery query = CQuery.with(new Triple(X, knows, Y), new Triple(ALICE, knows, X))
                .annotate(X, AtomAnnotation.asRequired(atom1))
                .annotate(Y, AtomAnnotation.asRequired(atom2))
                .annotate(ALICE, AtomAnnotation.of(atom3))
                .build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(X, BOB));

        assertEquals(bound.getResultVars(), singleton("y"));
        assertTrue(bound.hasInputs());
        assertEquals(bound.getInputVars(), singleton("y"));

        CQuery expected = CQuery.with(new Triple(BOB, knows, Y), new Triple(ALICE, knows, BOB))
                .annotate(BOB, AtomAnnotation.asRequired(atom1))
                .annotate(Y, AtomAnnotation.asRequired(atom2))
                .annotate(ALICE, AtomAnnotation.of(atom3))
                .build();
        assertEquals(bound.getQuery(), expected);
    }
}