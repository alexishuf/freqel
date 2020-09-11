package br.ufsc.lapesd.riefederator.model;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.jena.query.JenaSolution;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdBlank;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import com.google.common.collect.Sets;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJena;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class SPARQLStringTest implements TestContext {
    private final ARQEndpoint ep = ARQEndpoint.forModel(ModelFactory.createDefaultModel());
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
        if (expected == null) {
            expectThrows(IllegalArgumentException.class,
                         () -> SPARQLString.term2SPARQL(term, dict));
        } else {
            assertEquals(SPARQLString.term2SPARQL(term, dict), expected);
        }
    }

    @Test
    public void testTripleASK() {
        SPARQLString s = SPARQLString.create(createQuery(Alice, knows, Bob));
        assertTrue(s.isAsk());
        assertEquals(s.getVarNames(), emptySet());
    }

    @Test
    public void testConjunctiveASK() {
        SPARQLString s = SPARQLString.create(createQuery(Alice, knows, Bob, Alice, knows, Alice));
        assertTrue(s.isAsk());
        assertEquals(s.getVarNames(), emptySet());
    }

    @Test
    public void testTripleSELECT() {
        SPARQLString s = SPARQLString.create(createQuery(Alice, knows, new StdVar("who")));
        assertFalse(s.isAsk());
        assertEquals(s.getVarNames(), singleton("who"));
    }

    @Test
    public void testSELECTWithFilter() {
        SPARQLString sparqlString = SPARQLString.create(
                createQuery(x, age, y, SPARQLFilter.build("?y > 23")));
        assertFalse(sparqlString.isAsk());

        Model model = ModelFactory.createDefaultModel();
        model.add(toJena(Alice), FOAF.age, createTypedLiteral(24));
        model.add(toJena(Bob), FOAF.age, createTypedLiteral(22));

        String sparql = sparqlString.getSparql();
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
        CQuery query = createQuery(descriptive, PureDescriptive.INSTANCE, x, knows, Alice);
        SPARQLString string = SPARQLString.create(query);
        assertFalse(string.isAsk());
        String sparql = string.getSparql();
        assertFalse(sparql.contains("Bob"));
    }

    @Test
    public void testConjunctiveSELECTWithMissingInputAnnotation() throws SPARQLParseException {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, y,
                x, name, z, AtomInputAnnotation.asRequired(A1, "name").missingInResult().get());
        SPARQLString string = SPARQLString.create(query);
        assertFalse(string.isAsk());
        assertEquals(string.getVarNames(), Sets.newHashSet("x", "y", "z"));

        SPARQLParser parser = new SPARQLParser().allowExtraProjections(true);
        CQuery parsed = parser.parseConjunctive(string.getSparql());
        assertEquals(parsed.attr().getSet(), Sets.newHashSet(
                new Triple(Alice, knows, x),
                new Triple(x, age, y)));
    }

    @Test
    public void testDistinct() {
        String str = SPARQLString.create(createQuery(s, knows, o, Distinct.INSTANCE)).getSparql();
        assertTrue(Pattern.compile("SELECT +DISTINCT +").matcher(str).find());
    }

    @Test
    public void testProjection() {
        String str = SPARQLString.create(createQuery(s, knows, o, Projection.of("o"))).getSparql();
        assertTrue(Pattern.compile("SELECT +\\?o +WHERE").matcher(str).find());
    }

    @Test
    public void testLimit() {
        SPARQLString ss = SPARQLString.create(createQuery(Alice, knows, x, Limit.of(10)));
        assertEquals(ss.getVarNames(), singleton("x"));
        assertTrue(ss.getSparql().contains("LIMIT 10"));
        QueryFactory.create(ss.getSparql()); //throws if invalid syntax
    }

    @Test
    public void testAskWithVars() {
        String str = SPARQLString.create(createQuery(s, knows, o, Ask.INSTANCE)).getSparql();
        assertTrue(Pattern.compile("ASK +\\{").matcher(str).find());
    }

    private @Nonnull Model getRdf2() throws IOException {
        Model model = ModelFactory.createDefaultModel();
        try (InputStream stream = getClass().getResourceAsStream("../rdf-2.nt")) {
            assertNotNull(stream);
            RDFDataMgr.read(model, stream, Lang.TTL);
        }
        return model;
    }

    @Test
    public void testValuesSingleRowSingleVar() throws IOException {
        Model rdf2 = getRdf2();
        ValuesModifier m = new ValuesModifier(singleton("y"),
                                              singleton(MapSolution.build(y, lit(23))));
        CQuery query = createQuery(x, age, y, m);
        String sparql = SPARQLString.create(query).getSparql();
        Set<Term> actual = new HashSet<>();
        try (QueryExecution ex = QueryExecutionFactory.create(sparql, rdf2)) {
            ResultSet set = ex.execSelect();
            while (set.hasNext())
                actual.add(JenaWrappers.fromJena(set.next().get("x").asResource()));
        }

        assertEquals(actual, Sets.newHashSet(Alice, Charlie));
    }

    @Test
    public void testValues3Rows2Columns() throws IOException {
        Model rdf2 = getRdf2();
        ValuesModifier m = new ValuesModifier(asList("y", "z"), asList(
                MapSolution.builder().put(y, Person).put(z, lit(23)).build(),
                MapSolution.builder().put(y, Person).put(z, lit(25)).build(),
                MapSolution.builder().put(y, Person).put(z, lit(24)).build() //no result
        ));
        CQuery query = createQuery(x, type, y, x, age, z, m);
        String sparql = SPARQLString.create(query).getSparql();
        Set<Solution> actual = new HashSet<>();
        try (QueryExecution ex = QueryExecutionFactory.create(sparql, rdf2)) {
            ResultSet set = ex.execSelect();
            while (set.hasNext())
                actual.add(new JenaSolution(set.next()));
        }

        Set<Solution> expected = Sets.newHashSet(
                MapSolution.builder().put(x, Alice).put(y, Person).put(z, lit(23)).build(),
                MapSolution.builder().put(x, Charlie).put(y, Person).put(z, lit(23)).build(),
                MapSolution.builder().put(x, Dave).put(y, Person).put(z, lit(25)).build()
        );
        assertEquals(actual, expected);

        // should also work through ARQEndpoint
        actual.clear();
        ARQEndpoint ep = ARQEndpoint.forModel(rdf2);
        ep.query(query).forEachRemainingThenClose(actual::add);
        assertEquals(actual, expected);
    }

    @DataProvider
    public static Object[][] reParseData() {
        String prolog = "PREFIX ex: <"+ EX +">\n" +
                "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "PREFIX xsd: <"+ XSD.NS +">\n";
        return Stream.of(
                // simple queries with projections ...
                prolog+"SELECT * WHERE {?x ?p ?o .}",
                prolog+"SELECT ?x WHERE {?x ?p ?o .}",
                prolog+"SELECT ?x WHERE { ex:Alice foaf:knows ?x .}",
                prolog+"SELECT ?name WHERE { ex:Alice foaf:knows ?name .}",
                // filters ...
                prolog+"SELECT * WHERE { ex:Alice foaf:knows ?name ; foaf:age ?u FILTER(?u > 23).}",
                prolog+"SELECT ?name ?u WHERE { ex:Alice foaf:knows ?name ; foaf:age ?u FILTER(?u > 23).}",
                // LIMIT+DISTINCT
                prolog+"SELECT DISTINCT ?name WHERE {?x foaf:name ?name } LIMIT 10",
                // OPTIONAL
                prolog+"SELECT * WHERE {?x foaf:knows ?y . OPTIONAL {?y foaf:age ?u}}",
                prolog+"SELECT ?x ?u WHERE {?x foaf:knows ?y . OPTIONAL {?y foaf:age ?u}}",
                // UNION
                prolog+"SELECT * WHERE {\n" +
                        "  {\n" +
                        "    ?x foaf:knows ex:Alice " +
                        "  } UNION {\n" +
                        "    ?x foaf:knows ex:Bob " +
                        "  }\n" +
                        "}",
                // UNION with more triples and modifiers
                prolog+"SELECT * WHERE {\n" +
                        "  {\n" +
                        "    ?x foaf:knows ex:Alice " +
                        "  } UNION {\n" +
                        "    ?x foaf:knows ex:Bob " +
                        "  }\n" +
                        "}",
                // UNION with OPTIONAL within
                prolog+"SELECT ?u ?name WHERE {\n" +
                        "  {\n" +
                        "    ?x foaf:knows ex:Alice; foaf:age ?u FILTER(?u > 23) .\n" +
                        "  } UNION {\n" +
                        "    ?x foaf:knows ex:Bob; foaf:age ?u .\n" +
                        "    OPTIONAL { ?x foaf:name ?name }\n" +
                        "  }\n" +
                        "}",
                // join with OPTIONAL AND UNION with 3 operands + paths
                prolog+"SELECT ?x ?u ?thing ?type WHERE {\n" +
                        "  { {\n" +
                        "    ?x foaf:knows ex:Alice .\n" +
                        "    ex:Alice foaf:knows ?x .\n" +
                        "  } UNION {\n" +
                        "    ?x foaf:knows ex:Bob ; foaf:age ?u FILTER (?u < ?v) .\n" +
                        "    ex:Bob foaf:age ?v.\n" +
                        "  } UNION {\n" +
                        "    ?x foaf:knows ex:Charlie .\n" +
                        "    ex:Charlie foaf:knows/foaf:age ?u .\n" +
                        "  } } .\n" +
                        "  ?x foaf:knows/foaf:knows ?x ;\n" +
                        "     foaf:made ?thing .\n" +
                        "  OPTIONAL {?thing a ?type.}\n" +
                        "}"
        ).map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "reParseData")
    public void testReParse(@Nonnull String sparql) throws Exception {
        Op op = SPARQLParser.strict().parse(sparql);
        SPARQLString ss = SPARQLString.create(op);
        Op parsed = SPARQLParser.strict().parse(ss.getSparql());
        // compare some quick aspects of the trees
        assertEquals(parsed.getAllVars(), op.getAllVars());
        assertEquals(parsed.getResultVars(), op.getResultVars());
        assertEquals(parsed.getMatchedTriples(), op.getMatchedTriples());
        assertEquals(streamPreOrder(parsed).count(), streamPreOrder(op).count());

        // same histogram of Op classes
        Map<Class<? extends Op>, Integer> exHist = new HashMap<>(), acHist = new HashMap<>();
        streamPreOrder(op)
                .forEach(o -> exHist.put(o.getClass(), exHist.getOrDefault(o.getClass(), 0)+1));
        streamPreOrder(parsed)
                .forEach(o -> acHist.put(o.getClass(), acHist.getOrDefault(o.getClass(), 0)+1));
        assertEquals(acHist, exHist);

        // no modifier was lost
        Set<Modifier> exMods = new HashSet<>(), acMods = new HashSet<>();
        streamPreOrder(op).forEach(o -> exMods.addAll(o.modifiers()));
        streamPreOrder(parsed).forEach(o -> acMods.addAll(o.modifiers()));
        assertEquals(acMods, exMods);

        // full comparison. The previous tests serve mostly to help debug failures
        assertEquals(parsed, op);
    }

    @Test
    public void testRegressionLeftOptional() {
        JoinOp root = JoinOp.builder(
                new EndpointQueryOp(ep, createQuery(x, type, y, Optional.EXPLICIT)),
                new EndpointQueryOp(ep, createQuery(x, name, z))
        ).add(Projection.of("x", "y")).build();
        SPARQLString ss = SPARQLString.create(root);
        String sparql = ss.getSparql();
        assertTrue(Pattern.compile("SELECT\\s+\\?x\\s+\\?y\\s+WHERE").matcher(sparql).find());
        assertTrue(sparql.contains("OPTIONAL"));
        // OPTIONAL must appear after something else. It cannot appear at the star of a BGP
        // because that would mean it is optional in relation to nothing
        int zIndex = sparql.indexOf("?z");
        int optionalIndex = sparql.indexOf("OPTIONAL");
        assertTrue(zIndex > 0);
        assertTrue(optionalIndex > 0);
        assertTrue(zIndex < optionalIndex);
    }
}