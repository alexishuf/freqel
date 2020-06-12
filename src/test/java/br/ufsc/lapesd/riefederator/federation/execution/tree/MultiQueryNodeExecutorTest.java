package br.ufsc.lapesd.riefederator.federation.execution.tree;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class MultiQueryNodeExecutorTest implements TestContext {

    private static final List<NamedSupplier<MultiQueryNodeExecutor>> suppliers = singletonList(
            new NamedSupplier<>("SimpleQueryNodeExecutor",
                    () -> Guice.createInjector(Modules.override(new SimpleExecutionModule())
                            .with(new AbstractModule() {
                                    @Override
                                    protected void configure() {
                                        bind(MultiQueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
                                    }
                                })
                        ).getInstance(SimpleQueryNodeExecutor.class))
    );

    private static final @Nonnull ARQEndpoint rdf1, rdf2;

    static {
        rdf1 = loadRdf("rdf-1.nt");
        rdf2 = loadRdf("rdf-2.nt");
    }

    private static @Nonnull ARQEndpoint loadRdf(String filename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Model model = ModelFactory.createDefaultModel();
        String resourcePath = "br/ufsc/lapesd/riefederator/"+filename;
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
    public void testQueryBoth(Supplier<MultiQueryNodeExecutor> supplier) {
        MultiQueryNodeExecutor executor = supplier.get();
        QueryNode qn1 = new QueryNode(rdf1, createQuery(x, knows, Bob));
        QueryNode qn2 = new QueryNode(rdf2, createQuery(x, knows, Bob));

        Results results = executor.execute(MultiQueryNode.builder().add(qn1).add(qn2).build());
        Set<Solution> actual = new HashSet<>();
        results.forEachRemainingThenClose(actual::add);
        assertEquals(actual, Sets.newHashSet(
                MapSolution.build(x, Alice),
                MapSolution.build(x, Dave)
        ));
    }

    @Test(dataProvider = "testData")
    public void testRunFilters(Supplier<MultiQueryNodeExecutor> supplier) {
        MultiQueryNodeExecutor executor = supplier.get();
        QueryNode qn1 = new QueryNode(rdf1, createQuery(x, age, y));
        QueryNode qn2 = new QueryNode(rdf2, createQuery(x, age, y));
        MultiQueryNode node = MultiQueryNode.builder().add(qn1).add(qn2)
                                            .setResultVars(singleton("x")).build();
        node.addFilter(SPARQLFilter.builder("?y > 23").build());

        Results results = executor.execute(node);
        Set<Solution> actual = new HashSet<>();
        results.forEachRemainingThenClose(actual::add);
        assertEquals(actual, Sets.newHashSet(
                MapSolution.build(x, Dave)
        ));
    }
}
