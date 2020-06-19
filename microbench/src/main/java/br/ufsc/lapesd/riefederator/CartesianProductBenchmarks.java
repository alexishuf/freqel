package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.EagerCartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.LazyCartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.util.FusekiProcess;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.util.Modules;
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

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
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
        planExecutorEager = Guice.createInjector(Modules.override(new SimpleFederationModule() {
            @Override
            protected void configureResultsExecutor() {
                bind(ResultsExecutor.class).toInstance(resultsExecutor);
            }
        }).with(new AbstractModule() {
            @Override
            protected void configure() {
                bind(CartesianNodeExecutor.class).to(EagerCartesianNodeExecutor.class);
            }
        })).getInstance(PlanExecutor.class);
        planExecutorLazy = Guice.createInjector(Modules.override(new SimpleFederationModule() {
            @Override
            protected void configureResultsExecutor() {
                bind(ResultsExecutor.class).toInstance(resultsExecutor);
            }
        }).with(new AbstractModule() {
            @Override
            protected void configure() {
                bind(CartesianNodeExecutor.class).to(LazyCartesianNodeExecutor.class);
            }
        })).getInstance(PlanExecutor.class);

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
        QueryNode leftNode = new QueryNode(left, createQuery(x, fromJena(link1), y));
        QueryNode rightNode = new QueryNode(right, createQuery(u, fromJena(link2), v));
        CartesianNode node = new CartesianNode(Arrays.asList(leftNode, rightNode));
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
