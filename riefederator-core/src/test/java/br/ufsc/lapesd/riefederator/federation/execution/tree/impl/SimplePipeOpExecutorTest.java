package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation;
import br.ufsc.lapesd.riefederator.federation.execution.tree.PipeOpExecutor;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.parse.CQueryContext;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;

public class SimplePipeOpExecutorTest implements TestContext {
    private static final List<Supplier<PipeOpExecutor>> suppliers = Collections.singletonList(
            () -> SingletonSourceFederation.getInjector().getInstance(SimplePipeOpExecutor.class)
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
        op.modifiers().add(SPARQLFilter.build("?u > 23"));

        Set<Solution> actual = new HashSet<>();
        executor.execute(op).forEachRemainingThenClose(actual::add);
        assertEquals(actual, Sets.newHashSet(MapSolution.build(x, Dave)));
    }
}