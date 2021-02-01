package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimpleQueryOpExecutor;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import com.google.common.collect.Sets;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.ResultsAssert.assertExpectedResults;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.fail;

@Test(groups = {"fast"})
public class QueryOpExecutorTest implements TestContext {
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

    private static final List<NamedSupplier<QueryOpExecutor>> suppliers = asList(
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
    private static final @Nonnull ARQEndpoint rdf1;
    private static final @Nonnull ARQEndpoint rdf1WithoutFilters;

    static {
        resultExecutors = new ConcurrentLinkedQueue<>();

        Model model = new TBoxSpec().addResource(QueryOpExecutorTest.class, "../../../rdf-1.nt")
                                    .loadModel();
        rdf1 = ARQEndpoint.forModel(model, "rdf-1.nt");
        rdf1WithoutFilters = new ARQEndpoint("rdf-1.nt (no FILTER)",
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
        return suppliers.stream()
                .flatMap(s -> Stream.of(rdf1, rdf1WithoutFilters).map(e -> new Object[]{s, e}))
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void testExecuteQuery(Supplier<QueryOpExecutor> supplier, TPEndpoint ep) {
        QueryOpExecutor executor = supplier.get();
        Results results = executor.execute(new EndpointQueryOp(ep, createQuery(x, knows, Bob)));
        assertExpectedResults(results, singleton(MapSolution.build(x, Alice)));
    }

    @Test(dataProvider = "testData")
    public void testExecuteQueryWithFilter(Supplier<QueryOpExecutor> supplier, TPEndpoint ep) {
        QueryOpExecutor executor = supplier.get();
        CQuery qry = createQuery(x, name, y, JenaSPARQLFilter.build("REGEX(?y, \"^b.*\")"));

        Results results = executor.execute(new EndpointQueryOp(ep, qry));
        assertExpectedResults(results, Sets.newHashSet(
                MapSolution.builder().put(x, Bob).put(y, lit("bob", "en")).build(),
                MapSolution.builder().put(x, Bob).put(y, lit("beto", "pt")).build()
        ));
    }

    @Test(dataProvider = "testData")
    public void testPushesNodeFilterToQuery(Supplier<QueryOpExecutor> supplier, TPEndpoint ep) {
        QueryOpExecutor executor = supplier.get();
        EndpointQueryOp node = new EndpointQueryOp(ep, createQuery(x, name, y));
        node.modifiers().add(JenaSPARQLFilter.build("REGEX(?y, \"^b.*\")"));

        assertExpectedResults(executor.execute(node), Sets.newHashSet(
                MapSolution.builder().put(x, Bob).put(y, lit("bob", "en")).build(),
                MapSolution.builder().put(x, Bob).put(y, lit("beto", "pt")).build()
        ));
    }

    @Test(dataProvider = "testData")
    public void testFiltersInBothPlaces(Supplier<QueryOpExecutor> supplier, TPEndpoint ep) {
        QueryOpExecutor executor = supplier.get();
        CQuery qry = createQuery(x, name, y, JenaSPARQLFilter.build("REGEX(?y, \"^b.*$\")"));
        EndpointQueryOp node = new EndpointQueryOp(ep, qry);
        node.modifiers().add(JenaSPARQLFilter.build("REGEX(?y, \".*o$\")"));

        assertExpectedResults(executor.execute(node), singleton(
                MapSolution.builder().put(x, Bob).put(y, lit("beto", "pt")).build()
        ));
    }
}