package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.Sets;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.annotateTerm;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
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
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), emptySet());
        assertEquals(node.getInputVars(), emptySet());
        assertEquals(node.getPublicVars(), singleton("x"));
        assertEquals(node.getStrictResultVars(), singleton("x"));
        assertFalse(node.isProjecting());
    }

    @Test
    public void testVarInFilter() {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y, SPARQLFilter.build("?y < ?z")
        );
        QueryNode node = new QueryNode(empty, query);
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getAllVars(), Sets.newHashSet("x", "y", "z"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y")); //z is not input
        assertEquals(node.getInputVars(), emptySet());
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), emptySet());
    }


    @Test
    public void testRequiredInputInFilter() {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y, SPARQLFilter.build("?y < ?z"),
                annotateTerm(z, asRequired(atom1, "a1"))
        );
        QueryNode node = new QueryNode(empty, query);
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getStrictResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y", "z"));
        assertEquals(node.getRequiredInputVars(), singleton("z"));
        assertEquals(node.getOptionalInputVars(), emptySet());
        assertEquals(node.getInputVars(), singleton("z"));
    }

    @Test
    public void testOptionalInputInFilter() {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y, SPARQLFilter.build("?y < ?z"),
                annotateTerm(z, AtomAnnotation.asOptional(atom1, "a1"))
        );
        QueryNode node = new QueryNode(empty, query);
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getStrictResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y", "z"));
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), singleton("z"));
        assertEquals(node.getInputVars(), singleton("z"));
    }

    @Test
    public void testStrictResultVars() {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y, asRequired(atom1, "a2"),
                SPARQLFilter.build("?y < ?z"),
                annotateTerm(z, AtomAnnotation.asOptional(atom2, "a2"))
        );
        QueryNode node = new QueryNode(empty, query);
        assertFalse(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(node.getStrictResultVars(), singleton("x"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y", "z"));
        assertEquals(node.getRequiredInputVars(), singleton("y"));
        assertEquals(node.getOptionalInputVars(), singleton("z"));
        assertEquals(node.getInputVars(), Sets.newHashSet("y", "z"));
    }


    @Test
    public void testProject() {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y, asRequired(atom1, "a2"),
                SPARQLFilter.build("?y < ?z"),
                annotateTerm(z, AtomAnnotation.asOptional(atom2, "a2"))
        );
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        assertTrue(node.isProjecting());
        assertEquals(node.getResultVars(), Sets.newHashSet("x"));
        assertEquals(node.getStrictResultVars(), singleton("x"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y", "z"));
        assertEquals(node.getRequiredInputVars(), singleton("y"));
        assertEquals(node.getOptionalInputVars(), singleton("z"));
        assertEquals(node.getInputVars(), Sets.newHashSet("y", "z"));
    }

    @Test
    public void testProjectFilterInput() {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y,
                SPARQLFilter.build("?y < ?z"),
                annotateTerm(z, AtomAnnotation.asOptional(atom2, "a2"))
        );
        QueryNode node = new QueryNode(empty, query, singleton("x"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "z"));
        assertEquals(node.getResultVars(), singleton("x"));
        assertEquals(node.getInputVars(), singleton("z"));
        assertEquals(node.getRequiredInputVars(), emptySet());
        assertEquals(node.getOptionalInputVars(), singleton("z"));
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
                .annotate(y, asRequired(atom2, "atom2")).build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(x, Bob));

        assertEquals(bound.getResultVars(), singleton("y"));
        assertTrue(bound.hasInputs());
        assertEquals(bound.getRequiredInputVars(), singleton("y"));
        assertTrue(bound.getQuery().hasTermAnnotations());
        assertFalse(bound.getQuery().hasTripleAnnotations());
        assertEquals(bound.getQuery(), query);
    }

    @Test
    public void testBindingPreservesAnnotations() {
        CQuery query = CQuery.with(new Triple(x, knows, y), new Triple(Alice, knows, x))
                .annotate(x, asRequired(atom1, "atom1"))
                .annotate(y, asRequired(atom2, "atom2"))
                .annotate(Alice, AtomAnnotation.of(atom3))
                .build();
        QueryNode node = new QueryNode(empty, query);
        QueryNode bound = node.createBound(MapSolution.build(x, Bob));

        assertEquals(bound.getResultVars(), singleton("y"));
        assertTrue(bound.hasInputs());
        assertEquals(bound.getRequiredInputVars(), singleton("y"));

        CQuery expected = CQuery.with(new Triple(Bob, knows, y), new Triple(Alice, knows, Bob))
                .annotate(Bob, asRequired(atom1, "atom1"))
                .annotate(y, asRequired(atom2, "atom2"))
                .annotate(Alice, AtomAnnotation.of(atom3))
                .build();
        assertEquals(bound.getQuery(), expected);
    }

    @Test
    public void testBindInputInFilterOfQuery() {
        CQuery query = createQuery(
                x, age, y,
                SPARQLFilter.build("?y < ?u"),
                annotateTerm(u, asRequired(atom1, "a1"))
        );
        QueryNode node = new QueryNode(empty, query);
        assertEquals(node.getInputVars(), singleton("u"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y", "u"));
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y"));

        QueryNode bound = node.createBound(MapSolution.build(u, lit(23)));

        assertEquals(bound.getPublicVars(), Sets.newHashSet("x", "y"));
        assertEquals(bound.getInputVars(), emptySet());
        assertEquals(bound.getQuery().getModifiers().size(), 1);
        Modifier modifier = bound.getQuery().getModifiers().iterator().next();
        assertTrue(modifier instanceof SPARQLFilter);
        assertEquals(((SPARQLFilter)modifier).getVars(), singleton("y"));
    }

    @Test
    public void testBindInputInFilter() {
        CQuery query = createQuery(
                Alice, knows, x,
                Alice, age, y, SPARQLFilter.build("?y < ?u"),
                x, age, z, SPARQLFilter.build("?z < ?y"),
                annotateTerm(u, asRequired(atom1, "a1"))
        );
        QueryNode node = new QueryNode(empty, query);
        assertEquals(node.getInputVars(), singleton("u"));
        assertEquals(node.getPublicVars(), Sets.newHashSet("x", "y", "y", "z", "u"));
        assertEquals(node.getResultVars(), Sets.newHashSet("x", "y", "y", "z"));

        query.getModifiers().stream().filter(SPARQLFilter.class::isInstance)
                .forEach(m -> node.addFilter((SPARQLFilter)m));
        assertEquals(node.getFilers().size(), 2);

        QueryNode bound = node.createBound(MapSolution.build(u, lit(23)));
        assertEquals(bound.getInputVars(), emptySet());
        assertEquals(bound.getPublicVars(), Sets.newHashSet("x", "y", "y", "z"));

        assertEquals(bound.getQuery().getModifiers().size(), 2);
        assertTrue(bound.getQuery().getModifiers().contains(SPARQLFilter.build("?z < ?y")));
        assertTrue(bound.getQuery().getModifiers().contains(SPARQLFilter.build("?y < 23")));

        assertEquals(bound.getFilers().size(), 2);
        assertTrue(bound.getFilers().contains(SPARQLFilter.build("?z < ?y")));
        assertTrue(bound.getFilers().contains(SPARQLFilter.build("?y < 23")));
    }
}