package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.util.FusekiProcess;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Collections.singleton;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Thread)
public class BindJoinBenchmarks {
    private static final String EX = "http://example.org/ns#";
    private static final Property link1 = createProperty(EX+"link");
    private static final Property link2 = createProperty(EX+"link");
    private static final int ROWS = 128;

    private FusekiProcess leftFuseki, rightFuseki;
    private ARQEndpoint leftLocalEp, rightLocalEp;
    private SPARQLClient leftEp, rightEp, rightEpCannotValues;
    private PlanExecutor planExecutor;
    private ResultsExecutor resultsExec;
    private SequentialResultsExecutor seqResultsExec;
    private CQuery leftQuery, rightQuery;
    private final HashSet<String> resultVars = Sets.newHashSet("x", "y", "z");
    private final Set<String> joinVars = singleton("y");

    @Param({"10", "40", "80", "160"})
    private int valuesRows;

    @Param({"0.5", "1", "2"})
    private double rightSolutionRate;

    @Setup(Level.Trial)
    public void setUp() {
        Model left = ModelFactory.createDefaultModel();
        Model right = ModelFactory.createDefaultModel();
        for (int i = 0; i < ROWS; i++) {
            left.createResource(EX+i+"-1")
                    .addProperty(link1, left.createResource(EX+i+"-2"));
            int nRight = rightSolutionRate < 1
                    ? ( i % (int)Math.round(1/rightSolutionRate) == 0 ? 1 : 0 )
                    : (int)Math.round(rightSolutionRate);
            for (int j = 0; j < nRight; j++) {
                right.createResource(EX+i+"-2")
                        .addProperty(link2, right.createResource(EX+i+"-3-"+j));
            }
        }

        try {
            leftFuseki = new FusekiProcess(left);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start FusekiProcess for left");
        }
        try {
            rightFuseki = new FusekiProcess(right);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start FusekiProcess for right");
        }
        try {
            Thread.sleep(1000); //should not be necessary
        } catch (InterruptedException ignored) {}

        leftLocalEp = ARQEndpoint.forModel(left);
        rightLocalEp = ARQEndpoint.forModel(right);
        leftEp = new SPARQLClient(leftFuseki.getSparqlEndpoint());
        rightEp = new SPARQLClient(rightFuseki.getSparqlEndpoint());
        rightEpCannotValues = new SPARQLClient(rightFuseki.getSparqlEndpoint());
        rightEpCannotValues.removeRemoteCapability(Capability.VALUES);

        Injector injector = Guice.createInjector(new SimpleFederationModule());
        planExecutor = injector.getInstance(PlanExecutor.class);
        resultsExec = injector.getInstance(ResultsExecutor.class);
        seqResultsExec = new SequentialResultsExecutor();

        Var x = new StdVar("x"), y = new StdVar("y"), z = new StdVar("z");
        leftQuery = createQuery(x, fromJena(link1), y);
        rightQuery = createQuery(y, fromJena(link2), z);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        try {
            Thread.sleep(1000); //allow the child to clear up any backlog of tasks
        } catch (InterruptedException ignored) { }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        List<Exception> exceptions = new ArrayList<>();
        try {
            leftFuseki.close();
        } catch (Exception e) {
            exceptions.add(e);
            e.printStackTrace();
        }
        try {
            rightFuseki.close();
        } catch (Exception e) {
            exceptions.add(e);
            e.printStackTrace();
        }
        resultsExec.close();
        try {
            resultsExec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) { }
        seqResultsExec.close();
        if (exceptions.size() == 1) {
            throw exceptions.get(0);
        } else if (exceptions.size() > 1) {
            exceptions.subList(1, exceptions.size()).forEach(exceptions.get(0)::addSuppressed);
            throw exceptions.get(0);
        }
        leftEp.close();
        rightEp.close();
        rightEpCannotValues.close();
    }

    @Benchmark
    public Set<Solution> pathJoinWithValuesLocal() {
        EndpointQueryOp rightTree = new EndpointQueryOp(rightLocalEp, rightQuery);
        SimpleBindJoinResults joinResults = new SimpleBindJoinResults(planExecutor,
                leftLocalEp.query(leftQuery), rightTree,
                joinVars, resultVars, resultsExec, valuesRows);
        Set<Solution> set = new HashSet<>();
        joinResults.forEachRemainingThenClose(set::add);
        return set;
    }

    @Benchmark
    public Set<Solution> pathJoinWithValuesSequentialExecutorLocal() {
        EndpointQueryOp rightTree = new EndpointQueryOp(rightLocalEp, rightQuery);
        SimpleBindJoinResults joinResults = new SimpleBindJoinResults(planExecutor,
                leftLocalEp.query(leftQuery), rightTree,
                joinVars, resultVars, seqResultsExec, valuesRows);
        Set<Solution> set = new HashSet<>();
        joinResults.forEachRemainingThenClose(set::add);
        return set;
    }

    @Benchmark
    public Set<Solution> pathJoinWithValues() {
        EndpointQueryOp rightTree = new EndpointQueryOp(rightEp, rightQuery);
        SimpleBindJoinResults joinResults = new SimpleBindJoinResults(planExecutor,
                leftEp.query(leftQuery), rightTree,
                joinVars, resultVars, resultsExec, valuesRows);
        Set<Solution> set = new HashSet<>();
        joinResults.forEachRemainingThenClose(set::add);
        return set;
    }

    @Benchmark
    public Set<Solution> pathJoinWithValuesSequentialExecutor() {
        EndpointQueryOp rightTree = new EndpointQueryOp(rightEp, rightQuery);
        SimpleBindJoinResults joinResults = new SimpleBindJoinResults(planExecutor,
                leftEp.query(leftQuery), rightTree,
                joinVars, resultVars, seqResultsExec, valuesRows);
        Set<Solution> set = new HashSet<>();
        joinResults.forEachRemainingThenClose(set::add);
        return set;
    }

    @Benchmark
    public Set<Solution> pathJoinWithoutValues() {
        EndpointQueryOp rightTree = new EndpointQueryOp(rightEpCannotValues, rightQuery);
        SimpleBindJoinResults joinResults = new SimpleBindJoinResults(planExecutor,
                leftEp.query(leftQuery), rightTree,
                joinVars, resultVars, resultsExec, valuesRows);
        Set<Solution> set = new HashSet<>();
        joinResults.forEachRemainingThenClose(set::add);
        return set;
    }

    @Benchmark
    public Set<Solution> pathJoinWithoutValuesWithSequentialExecutor() {
        EndpointQueryOp rightTree = new EndpointQueryOp(rightEpCannotValues, rightQuery);
        SimpleBindJoinResults joinResults = new SimpleBindJoinResults(planExecutor,
                leftEp.query(leftQuery), rightTree,
                joinVars, resultVars, seqResultsExec, valuesRows);
        Set<Solution> set = new HashSet<>();
        joinResults.forEachRemainingThenClose(set::add);
        return set;
    }
}
