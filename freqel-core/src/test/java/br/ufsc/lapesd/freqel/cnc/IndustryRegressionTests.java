package br.ufsc.lapesd.freqel.cnc;

import br.ufsc.lapesd.freqel.TempDir;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.jena.JenaWrappers;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.webapis.requests.impl.ModelMessageBodyWriter;
import org.apache.commons.io.IOUtils;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Application;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.ResultsAssert.*;
import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.freqel.util.CollectionUtils.setMinus;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.*;

public class IndustryRegressionTests extends JerseyTestNg.ContainerPerClassTest
                                     implements TestContext {
    public static final String CNC = "https://frank.prof.ufsc.br/industry40/";
    public static final Var pt = new StdVar("pt");
    public static final Var c = new StdVar("c");
    public static final Var d = new StdVar("d");
    public static final Var t = new StdVar("t");
    public static final Var n = new StdVar("n");
    public static final Term value = new StdURI(CNC+"value");
    public static final Term id = new StdURI(CNC+"id");
    public static final Term cncType = new StdURI(CNC+"type");
    public static final Term parameter = new StdURI(CNC+"parameter");
    public static final Term machiningProcess = new StdURI(CNC+"machiningProcess");

    public static @Nonnull Term cnc(@Nonnull String localName) {
        return new StdURI(CNC+localName);
    }
    public @Nonnull Term cncAPI(@Nonnull String localName) {
        return new StdURI("http://"+host+"/cnc-api/"+localName);
    }

    private Federation federation;
    private TempDir tempDir;
    private String host;

    private void extractFiles() throws IOException {
        if (tempDir != null)
            return;
        tempDir = new TempDir();
        tempDir.extractResource(getClass(), "industry-scenario.yaml");
        tempDir.extractResource(getClass(), "experiment_01.csv");
        tempDir.extractResource(getClass(), "cnc-dtwin.json");
        tempDir.extractResource(getClass(), "industry4.0/cnc.json", "industry4.0/cnc.json");
        tempDir.extractResource(getClass(), "industry4.0/cnc.ttl", "industry4.0/cnc.ttl");
        tempDir.extractResource(getClass(), "industry4.0/cnc.yaml", "industry4.0/cnc.yaml");
    }

    @Override protected Application configure() {
        try {
            extractFiles();
        } catch (IOException e) {
            throw new RuntimeException("Failed to extractFiles()", e);
        }
        String dTwin = "cnc-dtwin.json", csv = "experiment_01.csv";
        CncService cncService = new CncService(dTwin, csv, ",");
        return new ResourceConfig().register(ModelMessageBodyWriter.class)
                                   .register(cncService);
    }

    @BeforeClass @Override public void setUp() throws Exception {
        super.setUp();
        extractFiles();

        File overlayFile = tempDir.getFile("industry4.0/cnc.yaml");
        URI rootUri = target().getUri();
        host = rootUri.getHost() + ":" + rootUri.getPort();
        String overlayContents;
        try (FileInputStream in = new FileInputStream(overlayFile)) {
            overlayContents = IOUtils.toString(in, UTF_8);
            overlayContents = overlayContents.replaceAll("localhost:9999", host);
            assertTrue(overlayContents.contains(host));
        }
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(overlayFile), UTF_8)) {
            writer.write(overlayContents);
        }
        try (FileInputStream in = new FileInputStream(overlayFile)) {
            assertTrue(IOUtils.toString(in, UTF_8).contains(host));
        }

        File spec = tempDir.getFile("industry-scenario.yaml");
        federation = new FederationSpecLoader().load(spec);
    }

    @AfterClass @Override public void tearDown() throws Exception {
        super.tearDown();
        if (tempDir != null) {
            tempDir.close();
            tempDir = null;
        }
        if (federation != null) {
            federation.close();
            federation = null;
        }
    }

    @DataProvider public @Nonnull Object[][] queryData() {
        String prefix = "PREFIX : <urn:plain:>\n" +
                "PREFIX cnc: <https://frank.prof.ufsc.br/industry40/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>";
        return Stream.of(
                asList("dump SP for 11", prefix+"SELECT * WHERE {?s ?p 11}",
                       singletonList(MapSolution.build(p, value, s, cnc("Exp01Sample01Param01"))),
                       emptyList(),
                       false),
                asList("dump", prefix+"SELECT * WHERE {?s ?p ?o}",
                       asList(MapSolution.build(s, cnc("name"),
                                                p, JenaWrappers.fromJena(RDFS.domain),
                                                o, cnc("Parameter")),
                              MapSolution.build(s, cnc("Exp01Sample01Param01"),
                                                p, value,
                                                o, integer(11)),
                              MapSolution.build(s, cncAPI("samples/102"),
                                                p, id, o, lit(102)),
                              MapSolution.build(s, cncAPI("samples/102"),
                                                p, machiningProcess, o, lit("Layer 1 Up")),
                              MapSolution.build(s, universalBlank,
                                                p, cncType, o, lit("S1_ActualAcceleration")),
                              MapSolution.build(s, universalBlank,
                                                p, value, o, lit(44.8))
                       ), emptyList(), false),
                  asList("low id, bound cnc:type", prefix+
                          "SELECT DISTINCT ?s ?id ?pt ?c ?d ?v ?u\n" +
                          "WHERE {\n" +
                          "  ?s cnc:id ?id ;\n" +
                          "     cnc:parameter ?p .\n" +
                          "  ?p cnc:value ?v ;\n" +
                          "     cnc:type \"X1_ActualVelocity\" .\n" +
                          "  ?pt rdfs:subClassOf ?c ;\n" +
                          "      cnc:name \"X1_ActualVelocity\" ;\n" +
                          "      cnc:description ?d ;\n" +
                          "      cnc:unit ?u .\n" +
                          "  ?c rdfs:subClassOf cnc:Parameter .\n" +
                          "  FILTER (?id <= 5)\n" +
                          "}\n",
                          asList(MapSolution.build(s,  cncAPI("samples/1"),
                                                   "id", lit(1),
                                                   v,  lit(-10.8),
                                                   pt, cnc("X1_ActualVelocity"),
                                                   c,  cnc("Velocity"),
                                                   d,  lit("actual x velocity of part"),
                                                   u,  lit("mm/s")),
                                 MapSolution.build(s,  cncAPI("samples/2"),
                                                   "id", lit(2),
                                                   v,  lit(-17.8),
                                                   pt, cnc("X1_ActualVelocity"),
                                                   c,  cnc("Velocity"),
                                                   d,  lit("actual x velocity of part"),
                                                   u,  lit("mm/s"))),
                          singletonList(
                                  // cnc.ttl uses rdf:type instead of cnc:type "X1_ActualVelocity"
                                  MapSolution.build(s,    cnc("Exp01Sample01"),
                                                    "id", lit(1),
                                                    v,    integer(11),
                                                    pt,   cnc("X1_ActualVelocity"),
                                                    c,    cnc("Velocity"),
                                                    d,    lit("actual x velocity of part"),
                                                    u,    lit("mm/s"))),
                          false),
                asList("low id, free cnc:type", prefix+
                                "SELECT DISTINCT ?s ?id ?pt ?c ?t ?d ?v ?u\n" +
                                "WHERE {\n" +
                                "  ?s cnc:id ?id ;\n" +
                                "     cnc:parameter ?p .\n" +
                                "  ?p cnc:value ?v ;\n" +
                                "     cnc:type ?t .\n" +
                                "  ?pt rdfs:subClassOf ?c ;\n" +
                                "      cnc:name ?t ;\n" +
                                "      cnc:description ?d ;\n" +
                                "      cnc:unit ?u .\n" +
                                "  ?c rdfs:subClassOf cnc:Parameter .\n" +
                                "  FILTER (?id <= 5)\n" +
                                "}\n",
                        asList(MapSolution.build(s,    cncAPI("samples/1"),
                                                 "id", lit(1),
                                                 v,    lit(-10.8),
                                                 pt,   cnc("X1_ActualVelocity"),
                                                 c,    cnc("Velocity"),
                                                 t,    lit("X1_ActualVelocity"),
                                                 d,    lit("actual x velocity of part"),
                                                 u,    lit("mm/s")),
                               MapSolution.build(s,    cncAPI("samples/2"),
                                                 "id", lit(2),
                                                 v,    lit(-17.8),
                                                 pt,   cnc("X1_ActualVelocity"),
                                                 c,    cnc("Velocity"),
                                                 t,    lit("X1_ActualVelocity"),
                                                 d,    lit("actual x velocity of part"),
                                                 u,    lit("mm/s"))
                        ),
                        // cnc.ttl uses rdf:type instead of cnc:type "X1_ActualVelocity"
                        asList(MapSolution.build(s,    cnc("Exp01Sample01"),
                                                 "id", lit(1),
                                                 v,    integer(11),
                                                 pt,   cnc("X1_ActualVelocity"),
                                                 c,    cnc("Velocity"),
                                                 t,    lit("X1_ActualVelocity"),
                                                 d,    lit("actual x velocity of part"),
                                                 u,    lit("mm/s")),
                               // previous entry tests for made-up ?t, test for missing ?t
                               MapSolution.build(s,    cnc("Exp01Sample01"),
                                                 "id", lit(1),
                                                 v,    integer(11),
                                                 pt,   cnc("X1_ActualVelocity"),
                                                 c,    cnc("Velocity"),
                                                 d,    lit("actual x velocity of part"),
                                                 u,    lit("mm/s"))),
                        false),
                asList("shorter low id, free cnc:type", prefix+"" +
                       "SELECT * WHERE {" +
                        "    ?s cnc:id ?id .\n" +
                        "    ?s cnc:parameter ?p .\n" +
                        "    ?p cnc:value ?v .\n" +
                        "    ?p cnc:type ?t .\n" +
                        "    ?pt cnc:name ?t .\n" +
                        "    FILTER(?id <= 2) }",
                        asList(MapSolution.build(s,    cncAPI("samples/1"),
                                                 "id", lit(1),
                                                 p,    universalBlank,
                                                 v,    lit(-10.8),
                                                 t,    lit("X1_ActualVelocity"),
                                                 pt,   cnc("X1_ActualVelocity")),
                               MapSolution.build(s,    cncAPI("samples/1"),
                                                 "id", lit(1),
                                                 p,    universalBlank,
                                                 v,    lit(0),
                                                 t,    lit("Z1_OutputVoltage"),
                                                 pt,   cnc("Z1_OutputVoltage"))),
                        singletonList(MapSolution.build(s,    cncAPI("samples/3"),
                                                        "id", lit(3),
                                                        p,    universalBlank,
                                                        v,    lit(112),
                                                        t,    lit("Z1_ActualPosition"),
                                                        pt,   cnc("Z1_ActualPosition"))),
                        false),
                asList("shorter low id, free cnc:type", prefix+"" +
                       "SELECT ?s ?v ?pt WHERE {" +
                       "    ?s cnc:id ?id .\n" +
                       "    ?s cnc:parameter ?p .\n" +
                       "    ?p cnc:value ?v .\n" +
                       "    ?p cnc:type \"X1_ActualVelocity\" .\n" +
                       "    ?pt cnc:name \"X1_ActualVelocity\" .\n" +
                       "    FILTER(?id <= 2) }",
                       asList(MapSolution.build(s,  cncAPI("samples/1"),
                                                v,  lit(-10.8),
                                                pt, cnc("X1_ActualVelocity")),
                              MapSolution.build(s,  cncAPI("samples/2"),
                                                v,  lit(-17.8),
                                                pt, cnc("X1_ActualVelocity"))),
                       asList(MapSolution.build(s,  cncAPI("samples/2"),
                                                v,  lit(0.524),
                                                pt, cnc("S1_CurrentFeedback")),
                              MapSolution.build(s, cncAPI("samples/3"),
                                                v, lit(-18),
                                                pt, cnc("X1_ActualVelocity"))),
                       false),
                asList("dump parameter classes", prefix+"SELECT ?n ?u WHERE {\n" +
                        "  ?pt rdfs:subClassOf ?c .\n" +
                        "  ?pt cnc:name ?n .\n" +
                        "  ?pt cnc:unit ?u .\n" +
                        "  ?c rdfs:subClassOf cnc:Parameter }",
                       asList(MapSolution.build(n, lit("X1_ActualAcceleration"),
                                                u, lit("mm/s^2")),
                              MapSolution.build(n, lit("X1_ActualVelocity"),
                                                u, lit("mm/s"))), emptyList(), false),
                asList("distinct units", prefix+"SELECT DISTINCT ?u WHERE {\n" +
                       "  ?pt rdfs:subClassOf ?c .\n" +
                       "  ?pt cnc:name ?n .\n" +
                       "  ?pt cnc:unit ?u .\n" +
                       "  ?c rdfs:subClassOf cnc:Parameter }",
                       asList(MapSolution.build(u, lit("mm")),
                              MapSolution.build(u, lit("mm/s")),
                              MapSolution.build(u, lit("mm/s^2")),
                              MapSolution.build(u, lit("A")),
                              MapSolution.build(u, lit("V")),
                              MapSolution.build(u, lit("kW")),
                              MapSolution.build(u, lit("kg*m^2")),
                              MapSolution.build(u, JenaWrappers.fromJena(RDF.nil))),
                       emptyList(), false)
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull String testName, @Nonnull String sparql,
                          @Nullable Collection<Solution> expected,
                          @Nullable Collection<Solution> forbidden,
                          boolean expectedComplete) throws SPARQLParseException {
        Op query = SPARQLParser.strict().parse(sparql);
        Op plan = federation.plan(query);
        ConjunctivePlannerTest.assertPlanAnswers(plan, query);
        List<Solution> list = new ArrayList<>();
        federation.execute(plan).forEachRemainingThenClose(list::add);
        if (expected != null) {
            if (expectedComplete) {
                assertExpectedResults(list, expected);
            } else {
                assertContainsResults(list, expected);
            }
        }
        if (forbidden != null)
            assertNotContainsResults(list, forbidden);
        if (expected == null || !expected.isEmpty()) {
            assertFalse(list.isEmpty(), "No results");
            if (streamPreOrder(query).noneMatch(o -> o.modifiers().optional() != null)) {
                Set<String> expectedVars = query.getPublicVars();
                for (Solution solution : list) {
                    assertEquals(setMinus(expectedVars, solution.getVarNames()), emptySet(),
                                 "Missing vars in solution");
                    assertEquals(setMinus(solution.getVarNames(), expectedVars), emptySet(),
                                 "Unexpected vars in solution");
                    for (String name : solution.getVarNames())
                        assertNotNull(solution.get(name), "Null value for var" + name);
                }
            }
        }
    }
}
