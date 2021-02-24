package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.deprecated.EagerCartesianOpExecutor;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.LazyCartesianOpExecutor;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.TestComponent;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.freqel.util.FusekiProcess;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.openjdk.jmh.annotations.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class CartesianProductBenchmarks {
    private static final String EX = "http://example.org/ns#";
    private static final Property link1 = createProperty(EX+"link");
    private static final Property link2 = createProperty(EX+"link");
    private static final Var x = new StdVar("x");
    private static final Var y = new StdVar("y");
    private static final Var u = new StdVar("u");
    private static final Var v = new StdVar("v");

    private CQEndpoint left, right;
    private PlanExecutor planExecutorLazy, planExecutorEager;
    private FusekiProcess leftFuseki;
    private FusekiProcess rightFuseki;
    private ResultsExecutor resultsExecutor;

    @Param({"1", "4", "16", "64"})
    private int sideSize;

    @Param({"false", "true"})
    private boolean local;

    @Param({"false", "true"})
    private boolean parallelResultsExecutor;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        Model left = ModelFactory.createDefaultModel(), right = ModelFactory.createDefaultModel();
        for (int i = 0; i < sideSize; i++) {
            left.add( createResource(EX+"left-s-" +i), link1, createResource(EX+"left-o-" +i));
            right.add(createResource(EX+"right-s-"+i), link2, createResource(EX+"right-o-"+i));
        }
        if (local) {
            this.left = ARQEndpoint.forModel(left);
            this.right = ARQEndpoint.forModel(right);
        } else {
            leftFuseki = new FusekiProcess(left);
            rightFuseki = new FusekiProcess(right);
            this.left = new SPARQLClient(leftFuseki.getSparqlEndpoint());
            this.right = new SPARQLClient(rightFuseki.getSparqlEndpoint());
        }
        resultsExecutor = parallelResultsExecutor ? new BufferedResultsExecutor()
                                                  : new SequentialResultsExecutor();
        TestComponent.Builder b = DaggerTestComponent.builder();
        b.overrideFreqelConfig(FreqelConfig.createDefault()
                .set(FreqelConfig.Key.CARTESIAN_OP_EXECUTOR, EagerCartesianOpExecutor.class));
        b.overrideResultsExecutor(resultsExecutor);
        planExecutorEager = b.build().planExecutor();
        b = DaggerTestComponent.builder();
        b.overrideFreqelConfig(FreqelConfig.createDefault()
                .set(FreqelConfig.Key.CARTESIAN_OP_EXECUTOR, LazyCartesianOpExecutor.class));
        b.overrideResultsExecutor(resultsExecutor);
        planExecutorLazy = b.build().planExecutor();

        if (!local) {
            this.left = new SPARQLClient(leftFuseki.getSparqlEndpoint());
            this.right = new SPARQLClient(rightFuseki.getSparqlEndpoint());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Exception exception = null;
        try {
           resultsExecutor.close();
        } catch (Exception e) {
            exception = e;
        }
        try {
            if (leftFuseki != null)
                leftFuseki.close();
        } catch (Exception e) {
            if (exception == null) exception = e;
            else exception.addSuppressed(e);
        }
        try {
            if (rightFuseki != null)
                rightFuseki.close();
        } catch (Exception e) {
            if (exception == null) exception = e;
            else exception.addSuppressed(e);
        }
        if (exception != null)
            throw exception;

        resultsExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    protected @Nonnull Set<Solution> runCartesian(@Nonnull PlanExecutor executor) {
        EndpointQueryOp leftNode = new EndpointQueryOp(left, createQuery(x, fromJena(link1), y));
        EndpointQueryOp rightNode = new EndpointQueryOp(right, createQuery(u, fromJena(link2), v));
        CartesianOp node = new CartesianOp(Arrays.asList(leftNode, rightNode));
        Results results = executor.executeNode(node);
        Set<Solution> set = new HashSet<>();
        results.forEachRemaining(set::add);
        int ex = sideSize * sideSize;
        if (set.size() != ex)
            throw new AssertionError("Expected "+ex+" results, got "+set.size());
        return set;
    }

    @Benchmark
    public Set<Solution> eagerCartesian() {
        return runCartesian(planExecutorEager);
    }

    @Benchmark
    public Set<Solution> lazyCartesian() {
        return runCartesian(planExecutorLazy);
    }
}
