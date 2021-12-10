package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.util.NamedFunction;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// extends CQ tests because upstream tests will be run through query(Op)
public class DQEndpointTest extends CQEndpointTest {
    private static final @Nonnull String PROLOG = "PREFIX ex: <"+EX+">\n" +
            "PREFIX foaf: <"+ FOAF.NS +">\n" +
            "PREFIX xsd: <"+ XSD.NS +">\n" +
            "PREFIX rdfs: <"+ RDFS.getURI() +">\n";

    private static final @Nonnull
    List<NamedFunction<InputStream, Fixture<CQEndpoint>>> endpoints = new ArrayList<>();

    static {
        for (NamedFunction<String, Fixture<TPEndpoint>> f : TPEndpointTest.endpoints) {
            if (f.toString().startsWith("ARQEndpoint") || f.toString().contains("SPARQL")) {
                //noinspection unchecked,rawtypes
                endpoints.add((NamedFunction) f);
            }
        }
    }

    @Override @DataProvider
    public Object[][] fixtureFactories() {
        return endpoints.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @Override
    protected void queryResourceTest(Function<String, Fixture<CQEndpoint>> f,
                                     @Nonnull Collection<Triple> query,
                                     @Nonnull Set<Solution> ex, boolean poll) {
        String filename = "rdf-2.nt";
        try (Fixture<CQEndpoint> fxt = f.apply(filename)) {
            Set<Solution> ac = new HashSet<>();
            assertTrue(fxt.endpoint instanceof DQEndpoint, "endpoint should to be a DQEndpoint");
            Op op = new EndpointQueryOp(fxt.endpoint, CQuery.from(query));
            try (Results results = ((DQEndpoint)fxt.endpoint).query(op)) {
                results.forEachRemaining(ac::add);
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected void querySPARQLTest(Function<String, Fixture<DQEndpoint>> f,
                                   @Nonnull String sparql,
                                   @Nonnull Set<Solution> ex) throws SPARQLParseException {
        String filename = "rdf-2.nt";
        Op query = SPARQLParser.strict().parse(sparql);
        try (Fixture<DQEndpoint> fxt = f.apply(filename)) {
            Set<Solution> ac = new HashSet<>();
            try (Results results = fxt.endpoint.query(query)) {
                results.forEachRemaining(ac::add);
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testUnion(Function<String, Fixture<DQEndpoint>> f) throws SPARQLParseException {
        querySPARQLTest(f, PROLOG+"SELECT ?x {\n" +
                "  {\n" +
                "    ?x foaf:age \"23\"^^xsd:int .\n" +
                "  } UNION {\n" +
                "    ?x foaf:age \"25\"^^xsd:int .\n" +
                "  }" +
                "}",
                Sets.newHashSet(
                        MapSolution.build(x, Alice),
                        MapSolution.build(x, Charlie),
                        MapSolution.build(x, Dave)
                ));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testOptional(Function<String, Fixture<DQEndpoint>> f) throws SPARQLParseException {
        querySPARQLTest(f, PROLOG+"SELECT * {\n" +
                "  ?x foaf:knows ?y .\n" +
                "  OPTIONAL { ?y foaf:age ?u }\n" +
                "}",
                Sets.newHashSet(
                        MapSolution.builder().put(x, Alice).put(y, Bob).put(u, null).build(),
                        MapSolution.builder().put(x, Dave).put(y, Bob).put(u, null).build()
                ));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testOptionalInsideUnion(Function<String, Fixture<DQEndpoint>> f) throws SPARQLParseException {
        Lit i25 = lit(25);
        querySPARQLTest(f, PROLOG+"SELECT ?x ?u ?y ?v {\n" +
                        "  {\n" +
                        "    {\n" +
                        "      ?x a foaf:Person . OPTIONAL {?x foaf:age ?u FILTER(?u > 23) }\n" +
                        "    } UNION {\n" +
                        "      ?x foaf:name \"bob\"@en \n" +
                        "    }\n" +
                        "  }\n" +
                        "  ?x foaf:knows ?y  OPTIONAL { ?y foaf:age ?v }\n" +
                        "}",
                Sets.newHashSet(
                        MapSolution.builder().put(x, Dave).put(u, i25).put(y, Bob)
                                             .put(v, null).build(),
                        MapSolution.builder().put(x, Alice).put(u, null).put(y, Bob)
                                             .put(v, null).build()
                ));
    }

}
