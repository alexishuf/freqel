package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.SimpleQueryOpExecutor;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.util.Modules;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

import static br.ufsc.lapesd.freqel.ResultsAssert.assertExpectedResults;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(groups = {"fast"})
public class UnionOpExecutorTest implements TestContext {

    private static final List<NamedSupplier<UnionOpExecutor>> suppliers = singletonList(
            new NamedSupplier<>("SimpleQueryNodeExecutor",
                    () -> Guice.createInjector(Modules.override(new SimpleExecutionModule())
                            .with(new AbstractModule() {
                                    @Override
                                    protected void configure() {
                                        bind(UnionOpExecutor.class).to(SimpleQueryOpExecutor.class);
                                    }
                                })
                        ).getInstance(SimpleQueryOpExecutor.class))
    );

    private static final @Nonnull ARQEndpoint rdf1, rdf2;

    static {
        rdf1 = loadRdf("rdf-1.nt");
        rdf2 = loadRdf("rdf-2.nt");
    }

    private static @Nonnull ARQEndpoint loadRdf(String filename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Model model = ModelFactory.createDefaultModel();
        String resourcePath = "br/ufsc/lapesd/freqel/"+filename;
        try (InputStream in = cl.getResourceAsStream(resourcePath)) {
            assertNotNull(in, "Resource " + resourcePath + "not found");
            RDFDataMgr.read(model, in, Lang.NT);
        } catch (IOException e) {
            fail("Unexpected IOException", e);
        }
        return ARQEndpoint.forModel(model, filename);
    }

    @DataProvider
    public static @Nonnull Object[][] testData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);

    }

    @Test(dataProvider = "testData")
    public void testQueryBoth(Supplier<UnionOpExecutor> supplier) {
        UnionOpExecutor executor = supplier.get();
        EndpointQueryOp qn1 = new EndpointQueryOp(rdf1, createQuery(x, knows, Bob));
        EndpointQueryOp qn2 = new EndpointQueryOp(rdf2, createQuery(x, knows, Bob));

        Results results = executor.execute(UnionOp.builder().add(qn1).add(qn2).build());
        assertExpectedResults(results, Sets.newHashSet(
                MapSolution.build(x, Alice),
                MapSolution.build(x, Dave)
        ));
    }

    @Test(dataProvider = "testData")
    public void testRunFilters(Supplier<UnionOpExecutor> supplier) {
        UnionOpExecutor executor = supplier.get();
        EndpointQueryOp qn1 = new EndpointQueryOp(rdf1, createQuery(x, age, y));
        EndpointQueryOp qn2 = new EndpointQueryOp(rdf2, createQuery(x, age, y));
        Op node = UnionOp.builder().add(qn1).add(qn2).build();
        node.modifiers().add(Projection.of("x"));
        node.modifiers().add(JenaSPARQLFilter.build("?y > 23"));

        assertExpectedResults(executor.execute(node), Sets.newHashSet(
                MapSolution.build(x, Dave)
        ));
    }
}