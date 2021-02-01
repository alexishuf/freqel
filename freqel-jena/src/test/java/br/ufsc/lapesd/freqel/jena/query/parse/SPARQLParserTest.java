package br.ufsc.lapesd.freqel.jena.query.parse;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.parse.UnsupportedSPARQLFeatureException;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.SPARQLAssert.assertTripleUniverse;
import static br.ufsc.lapesd.freqel.SPARQLAssert.assertVarsUniverse;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createTolerantQuery;
import static br.ufsc.lapesd.freqel.query.parse.SPARQLParser.hidden;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class SPARQLParserTest implements TestContext {

    @DataProvider
    public static @Nonnull Object[][] parseData() {
        String prolog = "PREFIX xsd: <" + XSD.getURI() + ">\n" +
                        "PREFIX rdf: <"+ RDF.getURI() +">\n" +
                        "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                        "PREFIX foaf: <"+ FOAF.getURI() +">\n" +
                        "PREFIX ex: <"+ TestContext.EX +">\n";
        return Stream.of(
                asList("", true, null, SPARQLParseException.class),
                asList("xxx", true, null, SPARQLParseException.class),
                asList("SELECT ?x WHERE {?x ex:p foaf:Agent.}", true, null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.}", true,
                       createQuery(TestContext.x, TestContext.p, TestContext.o),
                       null),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT 10", true,
                       createQuery(TestContext.x, TestContext.p, TestContext.o, Limit.of(10)),
                       null),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT -1", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT 0", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT ?x WHERE {?x ?p ?o.}", true,
                       createQuery(TestContext.x, TestContext.p, TestContext.o, Projection.of("x")),
                       null),
                asList(prolog+"ASK WHERE {?x ?p ?o.}", true,
                       createQuery(TestContext.x, TestContext.p, TestContext.o, Ask.INSTANCE),
                       null),
                asList(prolog+"ASK WHERE {?x rdf:type/rdfs:subClassOf* ?o.}", true,
                       null,
                       UnsupportedSPARQLFeatureException.class),
                asList("DESCRIBE <"+ TestContext.EX+"Alice>", true, null, UnsupportedSPARQLFeatureException.class),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(TestContext.Alice, TestContext.knows, TestContext.x, TestContext.x, TestContext.knows, TestContext.Bob),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               TestContext.Alice, TestContext.knows, TestContext.x,
                               TestContext.x, TestContext.age, TestContext.y, SPARQLFilterFactory.parseFilter("?y < 23"),
                               TestContext.x, TestContext.knows, TestContext.Bob),
                       null),
                asList(prolog+"SELECT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               TestContext.Alice, TestContext.knows, TestContext.x,
                               TestContext.x, TestContext.age, TestContext.y, SPARQLFilterFactory.parseFilter("?y < 23"),
                               TestContext.x, TestContext.knows, TestContext.Bob, Projection.of("x")),
                       null),
                asList(prolog+"SELECT DISTINCT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               TestContext.Alice, TestContext.knows, TestContext.x,
                               TestContext.x, TestContext.age, TestContext.y, SPARQLFilterFactory.parseFilter("?y < 23"),
                               TestContext.x, TestContext.knows, TestContext.Bob, Projection.of("x"), Distinct.INSTANCE),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                              "?x foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                TestContext.x, TestContext.knows, hidden(0),
                                hidden(0), TestContext.age, TestContext.u,
                                SPARQLFilterFactory.parseFilter("?u > 23"),
                                Projection.of("x", "u")),
                        null),
                asList(prolog+"SELECT * WHERE {\n" +
                              "?x foaf:name ?y; \n" +
                              "   foaf:knows/foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                TestContext.x, TestContext.name, TestContext.y,
                                TestContext.x, TestContext.knows, hidden(0),
                                hidden(0), TestContext.knows, hidden(1),
                                hidden(1), TestContext.age, TestContext.u,
                                SPARQLFilterFactory.parseFilter("?u > 23"),
                                Projection.of("x", "y", "u")),
                        null),
                asList(prolog+"SELECT DISTINCT ?y WHERE {\n" +
                                "?x foaf:name ?y; \n" +
                                "   foaf:knows/foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                TestContext.x, TestContext.name, TestContext.y,
                                TestContext.x, TestContext.knows, hidden(0),
                                hidden(0), TestContext.knows, hidden(1),
                                hidden(1), TestContext.age, TestContext.u,
                                SPARQLFilterFactory.parseFilter("?u > 23"),
                                Projection.of("y"), Distinct.INSTANCE),
                        null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "?x foaf:name ?y ; \n" +
                                "   foaf:knows/foaf:knows/foaf:age ?u ;\n" +
                                "   foaf:isPrimaryTopicOf/foaf:title ?z .\n}", true,
                        createQuery(
                                TestContext.x, TestContext.name, TestContext.y,
                                TestContext.x, TestContext.knows, hidden(0),
                                hidden(0), TestContext.knows, hidden(1),
                                hidden(1), TestContext.age, TestContext.u,
                                TestContext.x, TestContext.isPrimaryTopicOf, hidden(2),
                                hidden(2), TestContext.foafTitle, TestContext.z,
                                Projection.of("x", "y", "u", "z")),
                        null),
                asList(prolog+"SELECT ?x ?z WHERE {\n?x foaf:name ?y}", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT ?x ?z WHERE {\n?x foaf:name ?y}", false,
                       createTolerantQuery(TestContext.x, TestContext.name, TestContext.y, Projection.of("x", "z")), null),
                // simple union
                asList(prolog+"SELECT * WHERE {\n" +
                        "  { ?x foaf:knows ex:Alice . } UNION \n"+
                        "  { ?x foaf:knows ex:Bob. }\n" +
                        "}", true,
                        UnionOp.builder()
                                .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Alice)))
                                .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Bob)))
                                .build(), null),
                // filter placement in nodes
                asList(prolog+"SELECT * WHERE {\n" +
                                "  FILTER(REGEX(str(?x), \"^http:\")) \n" +
                                "  { ?x foaf:knows ex:Alice; \n" +
                                "       foaf:age ?y   FILTER(?y > 27) . } UNION \n"+
                                "  { ?x foaf:knows ex:Bob ; \n" +
                                "       foaf:age ?y . FILTER(?y > 23)   }\n" +
                                "  FILTER(?y < 40) .\n" +
                                "}", true,
                        UnionOp.builder()
                                .add(new QueryOp(
                                        createQuery(TestContext.x, TestContext.knows, TestContext.Alice,
                                                    TestContext.x, TestContext.age,   TestContext.y,
                                                    SPARQLFilterFactory.parseFilter("?y > 27"))))
                                .add(new QueryOp(
                                        createQuery(TestContext.x, TestContext.knows, TestContext.Bob,
                                                    TestContext.x, TestContext.age,   TestContext.y,
                                                    SPARQLFilterFactory.parseFilter("?y > 23")
                                        )))
                                .add(SPARQLFilterFactory.parseFilter("REGEX(str(?x), \"^http:\")"))
                                .add(SPARQLFilterFactory.parseFilter("?y < 40"))
                                .build(), null),
                // create a conjunction between a triple pattern and a UNION
                asList(prolog+"SELECT * WHERE {\n" +
                        "  ?x foaf:age ?u .\n" +
                        "  { ?x foaf:knows ex:Alice . } UNION { ?x foaf:knows ex:Bob }" +
                        "}", true,
                        ConjunctionOp.builder()
                                .add(new QueryOp(createQuery(TestContext.x, TestContext.age, TestContext.u)))
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Alice)))
                                        .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Bob)))
                                        .build())
                                .build(), null),
                // create a conjunction between a union and a triple block with a path expression
                asList(prolog+"SELECT * WHERE {\n" +
                                "  { \n" +
                                "    {\n" +
                                "      ?x foaf:knows ex:Alice .\n" +
                                "      ?x foaf:age ?u . FILTER(?u < ?y)\n" +
                                "    } UNION \n" +
                                "    {?x foaf:knows ex:Bob \n" +
                                "     FILTER(REGEX(str(?x), \"^http\")) .} \n" +
                                "  } .\n" +
                                "  ?x foaf:knows/foaf:age ?y FILTER (?y > 23)." +
                                "}", true,
                        ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(
                                                createQuery(TestContext.x, TestContext.knows, TestContext.Alice,
                                                            TestContext.x, TestContext.age,   TestContext.u,
                                                            SPARQLFilterFactory.parseFilter("?u < ?y"))))
                                        .add(new QueryOp(createQuery(
                                                TestContext.x, TestContext.knows, TestContext.Bob,
                                                SPARQLFilterFactory.parseFilter("REGEX(str(?x), \"^http\")"))))
                                        .build())
                                .add(new QueryOp(
                                        createQuery(TestContext.x,         TestContext.knows, TestContext.spHidden0,
                                                    TestContext.spHidden0,   TestContext.age, TestContext.y)))
                                .add(SPARQLFilterFactory.parseFilter("?y > 23"))
                                .add(Projection.of("x", "u", "y"))
                                .build(), null),
                // parse an OPTIONAL clause
                asList(prolog+"SELECT * WHERE {\n" +
                                "?x foaf:knows ex:Alice . OPTIONAL {?x foaf:name ?y .} }", true,
                       ConjunctionOp.builder()
                               .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Alice)))
                               .add(new QueryOp(createQuery(TestContext.x, TestContext.name, TestContext.y, Optional.EXPLICIT)))
                               .build(), null),
                // parse OPTIONAL with FILTER inside a UNION
                asList(prolog+"SELECT * WHERE {\n" +
                                " {\n" +
                                "    ?x foaf:knows ex:Alice .\n" +
                                "    OPTIONAL {\n" +
                                "      ?x foaf:name ?y .\n" +
                                "      ?x foaf:age ?u FILTER(?u > 23) .\n" +
                                "    }\n" +
                                "  } UNION {\n" +
                                "   ?x foaf:knows ex:Bob .\n" +
                                "  }\n" +
                                "}", true,
                        UnionOp.builder()
                                .add(ConjunctionOp.builder()
                                        .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Alice)))
                                        .add(new QueryOp(createQuery(
                                                TestContext.x, TestContext.name, TestContext.y,
                                                TestContext.x, TestContext.age, TestContext.u,
                                                SPARQLFilterFactory.parseFilter("?u > 23"), Optional.EXPLICIT
                                        ))).build())
                                .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Bob)))
                                .build(), null),
                // OPTIONAL in conjunction with UNION
                asList(prolog+"SELECT ?x ?u WHERE {\n" +
                                "  {\n" +
                                "    {\n" +
                                "      ?x foaf:knows ex:Alice .\n" +
                                "    } UNION {\n" +
                                "      ?x foaf:knows ex:Bob .\n" +
                                "    }\n" +
                                "  }\n" +
                                "  OPTIONAL { ?x foaf:name ?y ; foaf:age ?u FILTER(?u < 23)}\n" +
                                "}", true,
                        ConjunctionOp.builder()
                                .add(UnionOp.builder()
                                        .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Alice)))
                                        .add(new QueryOp(createQuery(TestContext.x, TestContext.knows, TestContext.Bob)))
                                        .build())
                                .add(new QueryOp(createQuery(
                                        TestContext.x, TestContext.name, TestContext.y,
                                        TestContext.x, TestContext.age, TestContext.u,
                                        SPARQLFilterFactory.parseFilter("?u < 23"), Optional.EXPLICIT)))
                                .add(Projection.of("u", "x"))
                                .build(), null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(@Nonnull String sparql, boolean strict, @Nullable Object expectedObj,
                          Class<? extends Throwable> exception) throws Exception {
        SPARQLParser parser = strict ? SPARQLParser.strict()
                                     : SPARQLParser.tolerant();
        if (exception == null) {
            Op query = parser.parse(sparql);
            Op expected = null;
            if (expectedObj instanceof CQuery)
                expected = new QueryOp((CQuery) expectedObj);
            else if (expectedObj instanceof Op)
                expected = (Op)expectedObj;
            else
                fail("Neither a CQuery nor an Op: expectedObj="+expectedObj);
            assertNotNull(expected); // bad test data
            assertEquals(query.getMatchedTriples(), expected.getMatchedTriples());
            assertEquals(query.modifiers(), expected.modifiers());

            //compare modifiers
            Set<Modifier> expectedModifiers = TreeUtils.streamPreOrder(expected)
                    .flatMap(o -> o.modifiers().stream()).collect(toSet());
            Set<Modifier> actualModifiers = TreeUtils.streamPreOrder(query)
                    .flatMap(o -> o.modifiers().stream()).collect(toSet());
            assertEquals(actualModifiers, expectedModifiers);

            //compare variables in root
            assertEquals(query.getResultVars(), expected.getResultVars());
            assertEquals(query.getStrictResultVars(), expected.getStrictResultVars());
            assertEquals(query.getInputVars(), expected.getInputVars());
            assertEquals(query.getRequiredInputVars(), expected.getRequiredInputVars());
            assertEquals(query.getOptionalInputVars(), expected.getOptionalInputVars());

            // compare whole tree (including variables and modifiers)
            //noinspection SimplifiedTestNGAssertion
            assertTrue(query.equals(expected));

            assertTripleUniverse(query);
            assertVarsUniverse(query);

            if (strict) // if strict was OK, tolerant should also be ok with it
                testParse(sparql, false, expected, null);
        } else {
            assertNull(expectedObj); //bad test data
            expectThrows(exception, () -> parser.parse(sparql));
        }
    }

}