package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLQueryParser;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import com.google.common.collect.Sets;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDFS;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJena;
import static br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict.EMPTY;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.testng.Assert.*;

public class SPARQLStringTest implements TestContext {
    private static final Atom A1 = new Atom("A1");

    @DataProvider
    public static Object[][] term2SPARQLData() {
        StdURI xsdInt = new StdURI(XSDDatatype.XSDint.getURI());
        return new Object[][] {
                new Object[] {new StdURI("http://example.org/A"), "<http://example.org/A>"},
                new Object[] {new StdURI(RDFS.Class.getURI()), "rdfs:Class"},
                new Object[] {new StdBlank(), "[]"},
                new Object[] {new StdBlank("bnode03"), "_:bnode03"},
                new Object[] {new StdVar("??asd"), null},
                new Object[] {new StdVar("x"), "?x"},
                new Object[] {StdLit.fromUnescaped("alice", "en"), "\"alice\"@en"},
                new Object[] {StdLit.fromUnescaped("23", xsdInt), "\"23\"^^xsd:int"},
        };
    }

    @Test(dataProvider = "term2SPARQLData")
    public void testTerm2SPARQL(@Nonnull Term term, @Nullable String expected) {
        PrefixDict dict = StdPrefixDict.STANDARD;
        if (expected == null)
            expectThrows(IllegalArgumentException.class, () -> SPARQLString.term2SPARQL(term, dict));
        else
            assertEquals(SPARQLString.term2SPARQL(term, dict), expected);
    }

    @Test
    public void testTripleASK() {
        SPARQLString s = new SPARQLString(singleton(new Triple(Alice, knows, Bob)), EMPTY);
        assertEquals(s.getType(), SPARQLString.Type.ASK);
        assertEquals(s.getVarNames(), emptySet());
    }

    @Test
    public void testConjunctiveASK() {
        SPARQLString s = new SPARQLString(asList(
                new Triple(Alice, knows, Bob), new Triple(Alice, knows, Alice)
        ), EMPTY);
        assertEquals(s.getType(), SPARQLString.Type.ASK);
        assertEquals(s.getVarNames(), emptySet());
    }

    @Test
    public void testTripleSELECT() {
        SPARQLString s = new SPARQLString(
                singleton(new Triple(Alice, knows, new StdVar("who"))), EMPTY);
        assertEquals(s.getType(), SPARQLString.Type.SELECT);
        assertEquals(s.getVarNames(), singleton("who"));
    }

    @Test
    public void testSELECTWithFilter() {
        SPARQLString sparqlString = new SPARQLString(
                createQuery(x, age, TestContext.y, SPARQLFilter.build("?y > 23")),
                EMPTY
        );
        assertEquals(sparqlString.getFilters(),
                     singleton(SPARQLFilter.build("?y > 23")));
        assertEquals(sparqlString.getType(), SPARQLString.Type.SELECT);

        Model model = ModelFactory.createDefaultModel();
        model.add(toJena(Alice), FOAF.age, createTypedLiteral(24));
        model.add(toJena(Bob), FOAF.age, createTypedLiteral(22));

        String sparql = sparqlString.getString();
        try (QueryExecution execution = QueryExecutionFactory.create(sparql, model)) {
            ResultSet results = execution.execSelect();
            assertTrue(results.hasNext());
            QuerySolution solution = results.next();
            assertEquals(solution.get("x"), toJena(Alice));
            assertEquals(solution.get("y"), createTypedLiteral(24));
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testConjunctiveSELECTWithPureDescriptive() {
        Triple descriptive = new Triple(x, knows, Bob);
        CQuery query = CQuery.with(descriptive, new Triple(x, knows, Alice))
                             .annotate(descriptive, PureDescriptive.INSTANCE).build();
        SPARQLString string = new SPARQLString(query, EMPTY, emptyList());
        assertEquals(string.getType(), SPARQLString.Type.SELECT);
        String sparql = string.getString();
        assertFalse(sparql.contains("Bob"));
    }

    @Test
    public void testConjunctiveSELECTWithMissingInputAnnotation() throws SPARQLParseException {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y,
                x, name, z, AtomInputAnnotation.asRequired(A1, "name").missingInResult().get());
        SPARQLString string = new SPARQLString(query, EMPTY, emptyList());
        assertEquals(string.getType(), SPARQLString.Type.SELECT);
        assertEquals(string.getVarNames(), Sets.newHashSet("x", "y"));
        assertEquals(string.getFilters(), emptySet());

        CQuery parsed = SPARQLQueryParser.parse(string.getString());
        assertEquals(parsed.getSet(), Sets.newHashSet(
                new Triple(Alice, knows, x),
                new Triple(x, age, y)));
    }

    @Test
    public void testDistinct() {
        StdVar s = new StdVar("s"), o = new StdVar("o");
        Set<Triple> qry = singleton(new Triple(s, knows, o));
        String str = new SPARQLString(qry, EMPTY, singleton(Distinct.REQUIRED)).toString();
        assertTrue(Pattern.compile("SELECT +DISTINCT +").matcher(str).find());
    }

    @Test
    public void testProjection() {
        StdVar s = new StdVar("s"), o = new StdVar("o");
        Set<Triple> qry = singleton(new Triple(s, knows, o));
        Projection mod = Projection.builder().add("o").build();
        String str = new SPARQLString(qry, EMPTY, singleton(mod)).toString();
        assertTrue(Pattern.compile("SELECT +\\?o +WHERE").matcher(str).find());
    }

    @Test
    public void testAskWithVars() {
        StdVar s = new StdVar("s"), o = new StdVar("o");
        Set<Triple> qry = singleton(new Triple(s, knows, o));
        String str = new SPARQLString(qry, EMPTY, singleton(Ask.REQUIRED)).toString();
        assertTrue(Pattern.compile("ASK +\\{").matcher(str).find());
    }
}