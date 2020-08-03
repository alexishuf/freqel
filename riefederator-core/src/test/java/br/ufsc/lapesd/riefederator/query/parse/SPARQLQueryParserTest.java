package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createTolerantQuery;
import static br.ufsc.lapesd.riefederator.query.parse.SPARQLQueryParser.hidden;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class SPARQLQueryParserTest implements TestContext {

    @DataProvider
    public static @Nonnull Object[][] parseData() {
        String prolog = "PREFIX xsd: <" + XSD.getURI() + ">\n" +
                        "PREFIX rdf: <"+ RDF.getURI() +">\n" +
                        "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                        "PREFIX foaf: <"+ FOAF.getURI() +">\n" +
                        "PREFIX ex: <"+ EX +">\n";
        return Stream.of(
                asList("", true, null, SPARQLParseException.class),
                asList("xxx", true, null, SPARQLParseException.class),
                asList("SELECT ?x WHERE {?x ex:p foaf:Agent.}", true, null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.}", true,
                       createQuery(x, p, o),
                       null),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT 10", true,
                       createQuery(x, p, o, Limit.advised(10)),
                       null),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT -1", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.} LIMIT 0", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT ?x WHERE {?x ?p ?o.}", true,
                       createQuery(x, p, o, Projection.required("x")),
                       null),
                asList(prolog+"ASK WHERE {?x ?p ?o.}", true,
                       createQuery(x, p, o, Ask.ADVISED),
                       null),
                asList(prolog+"ASK WHERE {?x rdf:type/rdfs:subClassOf* ?o.}", true,
                       null,
                       UnsupportedSPARQLFeatureException.class),
                asList("DESCRIBE <"+EX+"Alice>", true, null, UnsupportedSPARQLFeatureException.class),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(Alice, knows, x, x, knows, Bob),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob),
                       null),
                asList(prolog+"SELECT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob, Projection.required("x")),
                       null),
                asList(prolog+"SELECT DISTINCT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}", true,
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob, Projection.required("x"), Distinct.REQUIRED),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                              "?x foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                x, knows, hidden(0),
                                hidden(0), age, u,
                                SPARQLFilter.build("?u > 23"),
                                Projection.required("x", "u")),
                        null),
                asList(prolog+"SELECT * WHERE {\n" +
                              "?x foaf:name ?y; \n" +
                              "   foaf:knows/foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                x, name, y,
                                x, knows, hidden(0),
                                hidden(0), knows, hidden(1),
                                hidden(1), age, u,
                                SPARQLFilter.build("?u > 23"),
                                Projection.required("x", "y", "u")),
                        null),
                asList(prolog+"SELECT DISTINCT ?y WHERE {\n" +
                                "?x foaf:name ?y; \n" +
                                "   foaf:knows/foaf:knows/foaf:age ?u. FILTER(?u > 23)\n}", true,
                        createQuery(
                                x, name, y,
                                x, knows, hidden(0),
                                hidden(0), knows, hidden(1),
                                hidden(1), age, u,
                                SPARQLFilter.build("?u > 23"),
                                Projection.required("y"), Distinct.REQUIRED),
                        null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "?x foaf:name ?y ; \n" +
                                "   foaf:knows/foaf:knows/foaf:age ?u ;\n" +
                                "   foaf:isPrimaryTopicOf/foaf:title ?z .\n}", true,
                        createQuery(
                                x, name, y,
                                x, knows, hidden(0),
                                hidden(0), knows, hidden(1),
                                hidden(1), age, u,
                                x, isPrimaryTopicOf, hidden(2),
                                hidden(2), foafTitle, z,
                                Projection.required("x", "y", "u", "z")),
                        null),
                asList(prolog+"SELECT ?x ?z WHERE {\n?x foaf:name ?y}", true,
                       null, SPARQLParseException.class),
                asList(prolog+"SELECT ?x ?z WHERE {\n?x foaf:name ?y}", false,
                       createTolerantQuery(x, name, y, Projection.required("x", "z")), null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(@Nonnull String sparql, boolean strict, @Nullable CQuery expected,
                          Class<? extends Throwable> exception) throws Exception {
        SPARQLQueryParser parser = strict ? SPARQLQueryParser.strict()
                                          : SPARQLQueryParser.tolerant();
        if (exception == null) {
            CQuery query = parser.parse(sparql);
            assertNotNull(expected); // bad test data
            assertEquals(query.attr().getSet(), expected.attr().getSet());
            assertEquals(query.getModifiers(), expected.getModifiers());
            //noinspection SimplifiedTestNGAssertion
            assertTrue(query.equals(expected));

            if (strict) // if strict was OK, tolerant should also be ok with it
                testParse(sparql, false, expected, exception);
        } else {
            assertNull(expected); //bad test data
            expectThrows(exception, () -> parser.parse(sparql));
        }
    }
}