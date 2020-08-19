package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

/**
 * Extremely simple end-to-end test for optional queries.
 *
 * Testing variations of federation components and interaction with other constraints falls
 * into {@link FederationTest} scope
 */
public class OptionalTest implements TestContext {

    @DataProvider
    public static @Nonnull Object[][] optionalData() {
        Lit[] i = new Lit[10];
        for (int j = 0; j < 10; j++)
            i[j] = StdLit.fromUnescaped(String.valueOf(j), xsdInteger);
        String prolog = "PREFIX rdf: <"+ RDF.getURI() +">\n" +
                "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                "PREFIX xsd: <"+ XSD.NS +">\n" +
                "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "PREFIX ex: <"+ EX +">\n";
        return Stream.of(
                // no optional, but ensures the test itself is working
                asList(prolog+"SELECT ?x WHERE {?x foaf:age 23.}",
                       singletonList(MapSolution.build(x, Alice))),
                // simple optional
                asList(prolog+"SELECT ?x ?y WHERE {?x foaf:name ?name . OPTIONAL {?x a ?y}}",
                        asList(MapSolution.builder().put(x, Alice).put(y, Person).build(),
                               MapSolution.build(x, Bob))),
                // optional with 2 TPs
                asList(prolog+"SELECT ?x ?y WHERE {\n" +
                                "  ex:l1 ex:p1 ?x.\n" +
                                "  OPTIONAL {?w ex:p1 ?x; ex:p2 ?y.}\n" +
                                "}",
                        asList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build(),
                               MapSolution.builder().put(x, i[1]).put(y, i[4]).build(),
                               MapSolution.builder().put(x, i[2]).put(y, null).build())),
                // optional with 2 TPs that cause a product
                asList(prolog+"SELECT * WHERE {\n" +
                              "  ex:l1 ex:p1 ?x.\n" +
                              "  OPTIONAL {ex:r1 ex:p1 ?x; ex:p2 ?y.}\n" +
                              "}",
                       asList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build(),
                              MapSolution.builder().put(x, i[1]).put(y, i[4]).build(),
                              MapSolution.builder().put(x, i[2]).put(y, null).build()))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "optionalData", groups = {"fast"})
    public void testOptionalSingleSource(@Nonnull String sparql,
                                         @Nonnull List<Solution> expectedList)
            throws SPARQLParseException {
        Op query = SPARQLParser.strict().parse(sparql);
        try (Federation federation = Federation.createDefault()) {
            ARQEndpoint ep = ARQEndpoint.forModel(new TBoxSpec()
                    .addResource(getClass(), "rdf-optional-1.ttl").loadModel());
            federation.addSource(new Source(new SelectDescription(ep), ep));

            Results results = federation.query(query);
            List<Solution> list = new ArrayList<>();
            results.forEachRemainingThenClose(list::add);

            HashSet<Solution> expectedSet = new HashSet<>(expectedList);
            assertEquals(new HashSet<Solution>(list), expectedSet);
            assertEquals(list.size(), expectedList.size());
        }
    }
}
