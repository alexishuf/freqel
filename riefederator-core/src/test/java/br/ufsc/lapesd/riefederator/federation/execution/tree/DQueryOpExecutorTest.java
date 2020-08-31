package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleQueryOpExecutor;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import com.google.common.collect.Sets;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class DQueryOpExecutorTest implements TestContext {
    private static final @Nonnull String PROLOG = "PREFIX xsd: <"+ XSD.NS +">\n" +
            "PREFIX foaf: <"+ FOAF.NS+">\n" +
            "PREFIX ex: <"+EX+">\n";

    private static final PlanExecutor failExecutor = new PlanExecutor() {
        @Override
        public @Nonnull Results executePlan(@Nonnull Op plan) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @Nonnull Results executeNode(@Nonnull Op node) {
            throw new UnsupportedOperationException();
        }
    };

    private static @Nonnull ResultsExecutor saveExecutor(@Nonnull ResultsExecutor executor) {
        resultExecutors.add(executor);
        return executor;
    }

    private static final List<NamedSupplier<DQueryOpExecutor>> suppliers = asList(
            new NamedSupplier<>("SimpleQueryNodeExecutor+SequentialResultsExecutor",
                    () -> new SimpleQueryOpExecutor(failExecutor,
                            saveExecutor(new SequentialResultsExecutor()))),
            new NamedSupplier<>("SimpleQueryNodeExecutor+BufferedResultsExecutor(single, 1)",
                    () -> new SimpleQueryOpExecutor(failExecutor,
                            saveExecutor(new BufferedResultsExecutor(Executors.newSingleThreadExecutor(), 1)))),
            new NamedSupplier<>("SimpleQueryNodeExecutor+BufferedResultsExecutor(single, 10)",
                    () -> new SimpleQueryOpExecutor(failExecutor,
                            saveExecutor(new BufferedResultsExecutor(Executors.newSingleThreadExecutor(), 10)))),
            new NamedSupplier<>("SimpleQueryNodeExecutor+BufferedResultsExecutor",
                    () -> new SimpleQueryOpExecutor(failExecutor,
                            saveExecutor(new BufferedResultsExecutor())))
    );

    private static final @Nonnull Queue<ResultsExecutor> resultExecutors;
    private static final @Nonnull ARQEndpoint rdf2;
    private static final @Nonnull ARQEndpoint rdf2WithoutFilters;

    static {
        resultExecutors = new ConcurrentLinkedQueue<>();

        Model model = new TBoxSpec().addResource(QueryOpExecutorTest.class, "../../../rdf-2.nt")
                .loadModel();
        rdf2 = ARQEndpoint.forModel(model, "rdf-2.nt");
        rdf2WithoutFilters = new ARQEndpoint("rdf-2.nt (no FILTER)",
                q -> QueryExecutionFactory.create(q, model), null,
                () -> {}, false) {
            @Override
            public @Nonnull Results query(@Nonnull CQuery query) {
                if (query.getModifiers().stream().anyMatch(SPARQLFilter.class::isInstance))
                    fail("This endpoint does not support FILTER! QueryNodeExecutor is bugged");
                return super.query(query);
            }

            @Override
            public boolean hasSPARQLCapabilities() {
                return false;
            }

            @Override
            public boolean hasRemoteCapability(@Nonnull Capability capability) {
                if (capability == Capability.SPARQL_FILTER) return false;
                return super.hasRemoteCapability(capability);
            }
        };
    }

    @AfterMethod
    public void tearDown() {
        for (ResultsExecutor e = resultExecutors.poll(); e != null; e = resultExecutors.poll())
            e.close();
    }

    @DataProvider
    public static Object[][] testData() {
        return Stream.of(
                // simplest case: could be a CQuery
                asList(PROLOG+"SELECT * WHERE {?x foaf:age \"25\"^^xsd:int ; foaf:knows ex:Bob.}",
                       false, singleton(MapSolution.build(x, Dave))),
                // Optional modifier is carried to Results
                asList(PROLOG+"SELECT * WHERE {?x foaf:age \"25\"^^xsd:int ; foaf:knows ex:Bob.}",
                       true, singleton(MapSolution.build(x, Dave))),
                // use OPTIONAL within query, but not at root
                asList(PROLOG+"SELECT ?y ?u WHERE {\n" +
                                "  ?x foaf:knows ?y OPTIONAL {?y foaf:age ?u}\n" +
                                "}",
                        false, singleton(MapSolution.builder().put(y, Bob).put(u, null).build())),
                // UNION
                asList(PROLOG+"SELECT ?x WHERE {\n" +
                        "  {\n" +
                        "    ?x foaf:age \"23\"^^xsd:int ." +
                        "  } UNION {\n" +
                        "    ?x foaf:age \"25\"^^xsd:int ." +
                        "  }\n" +
                        "}", false, Sets.newHashSet(
                                MapSolution.build(x, Alice),
                                MapSolution.build(x, Charlie),
                                MapSolution.build(x, Dave))),
                // UNION + optional
                asList(PROLOG+"SELECT ?x WHERE {\n" +
                        "  {\n" +
                        "    ?x foaf:age \"23\"^^xsd:int ." +
                        "  } UNION {\n" +
                        "    ?x foaf:age \"25\"^^xsd:int ." +
                        "  }\n" +
                        "}", true, Sets.newHashSet(
                        MapSolution.build(x, Alice),
                        MapSolution.build(x, Charlie),
                        MapSolution.build(x, Dave))),
                // UNION with FILTER in operand and optional at root
                asList(PROLOG+"SELECT ?x WHERE {\n" +
                        "  {\n" +
                        "    ?x foaf:age \"23\"^^xsd:int ;\n" +
                        "       foaf:knows/foaf:name ?name FILTER(str(?name)=\"bob\") ." +
                        "  } UNION {\n" +
                        "    ?x foaf:age \"25\"^^xsd:int ." +
                        "  }\n" +
                        "}", true, Sets.newHashSet(
                        MapSolution.build(x, Alice),
                        MapSolution.build(x, Dave)))
                ).flatMap(l -> suppliers.stream()
                .flatMap(s -> Stream.of(rdf2, rdf2WithoutFilters)
                        .map(e -> {
                            ArrayList<Object> copy = new ArrayList<>(l);
                            copy.add(0, e);
                            copy.add(0, s);
                            return copy;
                        }))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Supplier<DQueryOpExecutor> supplier,
                     @Nonnull DQEndpoint ep, @Nonnull String sparql, boolean optional,
                     @Nonnull Set<Solution> expected) throws SPARQLParseException {
        DQueryOpExecutor exec = supplier.get();
        Op query = SPARQLParser.strict().parse(sparql);
        DQueryOp op = new DQueryOp(ep, query);
        if (optional)
            op.modifiers().add(Optional.INSTANCE);
        Set<Solution> actual = new HashSet<>();
        Results results = exec.execute(op);
        assertEquals(results.isOptional(), optional);

        results.forEachRemainingThenClose(actual::add);
        assertEquals(actual, expected);
    }

}
