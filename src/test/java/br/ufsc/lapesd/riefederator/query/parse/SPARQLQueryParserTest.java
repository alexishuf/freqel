package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Ask;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
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
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

public class SPARQLQueryParserTest implements TestContext {

    @DataProvider
    public static @Nonnull Object[][] parseData() {
        String prolog = "PREFIX xsd: <" + XSD.getURI() + ">\n" +
                        "PREFIX rdf: <"+ RDF.getURI() +">\n" +
                        "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                        "PREFIX foaf: <"+ FOAF.getURI() +">\n" +
                        "PREFIX ex: <"+ EX +">\n";
        return Stream.of(
                asList("", null, SPARQLParseException.class),
                asList("xxx", null, SPARQLParseException.class),
                asList("SELECT ?x WHERE {?x ex:p foaf:Agent.}", null, SPARQLParseException.class),
                asList(prolog+"SELECT * WHERE {?x ?p ?o.}",
                       createQuery(x, p, o),
                       null),
                asList(prolog+"SELECT ?x WHERE {?x ?p ?o.}",
                       createQuery(x, p, o, Projection.required("x")),
                       null),
                asList(prolog+"ASK WHERE {?x ?p ?o.}",
                       createQuery(x, p, o, Ask.ADVISED),
                       null),
                asList(prolog+"ASK WHERE {?x rdf:type/rdfs:subClassOf* ?o.}",
                       null,
                       UnsupportedSPARQLFeatureException.class),
                asList("DESCRIBE <"+EX+"Alice>", null, UnsupportedSPARQLFeatureException.class),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:knows ex:Bob.\n}",
                       createQuery(Alice, knows, x, x, knows, Bob),
                       null),
                asList(prolog+"SELECT * WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}",
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob),
                       null),
                asList(prolog+"SELECT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}",
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob, Projection.required("x")),
                       null),
                asList(prolog+"SELECT DISTINCT ?x WHERE {\n" +
                                "ex:Alice foaf:knows ?x.\n" +
                                "?x foaf:age ?y\n" +
                                "  FILTER(?y < 23).\n" +
                                "?x foaf:knows ex:Bob.\n}",
                       createQuery(
                               Alice, knows, x,
                               x, age, y, SPARQLFilter.build("?y < 23"),
                               x, knows, Bob, Projection.required("x"), Distinct.REQUIRED),
                       null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(@Nonnull String sparql, @Nullable CQuery expected,
                          Class<? extends Throwable> exception) throws Exception {
        if (exception == null) {
            CQuery query = SPARQLQueryParser.parse(sparql);
            assertNotNull(expected); // bad test data
            assertEquals(query.getSet(), expected.getSet());
            assertEquals(query.getModifiers(), expected.getModifiers());
            //noinspection SimplifiedTestNGAssertion
            assertTrue(query.equals(expected));
        } else {
            assertNull(expected); //bad test data
            expectThrows(exception, () -> SPARQLQueryParser.parse(sparql));
        }
    }
}