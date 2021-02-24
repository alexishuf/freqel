package br.ufsc.lapesd.freqel.federation.execution.tree.impl;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.federation.execution.tree.PipeOpExecutor;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.parse.CQueryContext;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static br.ufsc.lapesd.freqel.ResultsAssert.assertExpectedResults;
import static java.util.Collections.singleton;

public class SimplePipeOpExecutorTest implements TestContext {
    private static final List<Supplier<PipeOpExecutor>> suppliers = Collections.singletonList(
            () -> DaggerTestComponent.builder().build().simplePipeOpExecutor()
    );

    @DataProvider public static Object[][] applyFilterData() {
        return suppliers.stream().map(s -> new Object[] {s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "applyFilterData")
    public void testApplyFilter(@Nonnull Supplier<PipeOpExecutor> supplier)  {
        PipeOpExecutor executor = supplier.get();
        Model model = new TBoxSpec().addResource(getClass(), "../../../../rdf-2.nt").loadModel();
        ARQEndpoint ep = ARQEndpoint.forModel(model);
        EndpointQueryOp child = new EndpointQueryOp(ep, CQueryContext.createQuery(x, age, u));
        PipeOp op = new PipeOp(child);
        op.modifiers().add(Projection.of("x"));
        op.modifiers().add(JenaSPARQLFilter.build("?u > 23"));

        assertExpectedResults(executor.execute(op), singleton(MapSolution.build(x, Dave)));
    }
}