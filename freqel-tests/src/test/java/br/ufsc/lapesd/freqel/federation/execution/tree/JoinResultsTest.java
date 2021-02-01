package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.ResultsAssert;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.federation.SingletonSourceFederation;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.model.term.Lit;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.modifiers.Optional;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.util.CollectionUtils;
import com.google.inject.Injector;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JoinResultsTest implements TestContext {

    public static final Injector injector = SingletonSourceFederation.getInjector();
    public static final QueryOpExecutor opExecutor = injector.getInstance(QueryOpExecutor.class);
    public static final PlanExecutor planExecutor = injector.getInstance(PlanExecutor.class);

    public static abstract class JoinFactory
            implements BiFunction<EndpointQueryOp, EndpointQueryOp, Results> {
        private final @Nonnull String name;

        protected JoinFactory(@Nonnull String name) {
            this.name = name;
        }

        public boolean canLeftOptional() {
            return true;
        }
        protected @Nonnull Results ex(@Nonnull EndpointQueryOp op) {
            Results results = opExecutor.execute(op);
            assertEquals(results.isOptional(), op.modifiers().optional() != null);
            return results;
        }
        public @Nonnull Set<String> joinVars(@Nonnull Op left, @Nonnull Op right) {
            return CollectionUtils.intersect(left.getPublicVars(), right.getPublicVars());
        }
        public @Nonnull Set<String> resultVars(@Nonnull Op left, @Nonnull Op right) {
            return CollectionUtils.union(left.getPublicVars(), right.getPublicVars());
        }

        @Override public String toString() {
            return name;
        }
    }

    public static List<JoinFactory> factories = asList(
            new JoinFactory("InMemoryHashJoinResults without threads") {
                @Override public Results apply(EndpointQueryOp left, EndpointQueryOp right) {
                    return new InMemoryHashJoinResults(ex(left), ex(right), joinVars(left, right),
                                                       resultVars(left, right), false);
                }
            },
            new JoinFactory("InMemoryHashJoinResults with threads") {
                @Override public Results apply(EndpointQueryOp left, EndpointQueryOp right) {
                    return new InMemoryHashJoinResults(ex(left), ex(right), joinVars(left, right),
                                                       resultVars(left, right), true);
                }
            },
            new JoinFactory("ParallelInMemoryHashJoinResults") {
                @Override
                public Results apply(EndpointQueryOp l, EndpointQueryOp r) {
                    return new ParallelInMemoryHashJoinResults(ex(l), ex(r), joinVars(l, r),
                                                               resultVars(l, r));
                }
            },
            new JoinFactory("SimpleBindJoinResults + SequentialResultsExecutor") {
                @Override
                public Results apply(EndpointQueryOp l, EndpointQueryOp r) {
                    return new SimpleBindJoinResults(planExecutor, ex(l), r, joinVars(l, r),
                            resultVars(l, r), new SequentialResultsExecutor(),
                            SimpleBindJoinResults.DEF_VALUES_ROWS);
                }

                @Override public boolean canLeftOptional() {
                    return false;
                }
            },
            new JoinFactory("SimpleBindJoinResults + BufferedResultsExecutor") {
                @Override
                public Results apply(EndpointQueryOp l, EndpointQueryOp r) {
                    BufferedResultsExecutor resultsExecutor = new BufferedResultsExecutor();
                    return new SimpleBindJoinResults(planExecutor, ex(l), r, joinVars(l, r),
                                                     resultVars(l, r), resultsExecutor,
                                                     SimpleBindJoinResults.DEF_VALUES_ROWS) {
                        @Override public void close() throws ResultsCloseException {
                            super.close();
                            resultsExecutor.close();
                        }
                    };
                }

                @Override public boolean canLeftOptional() {
                    return false;
                }
            }

    );

    private List<ARQEndpoint> eps;
    private ExecutorService executor;

    @BeforeClass(groups = {"fast"})
    public void beforeClass() {
        eps = new ArrayList<>();
        Model join1 = new TBoxSpec().addResource(getClass(), "join-1.ttl")
                .loadModel();
        eps.add(ARQEndpoint.forModel(join1, "join-1"));
        eps.add(new ARQEndpoint("join-1[no VALUES]",
                q -> QueryExecutionFactory.create(q, join1), null,
                () -> {}, true) {
            @Override public boolean hasRemoteCapability(@Nonnull Capability capability) {
                if (capability == Capability.VALUES)
                    return false;
                return super.hasRemoteCapability(capability);
            }
        });
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterClass(groups = {"fast"})
    public void afterClass() throws InterruptedException {
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @DataProvider public static Object[][] testData() {
        String prolog = "PREFIX rdfs: <"+ RDFS.getURI() +">\n" +
                "PREFIX rdf: <"+ RDF.getURI() +">\n" +
                "PREFIX xsd: <"+ XSD.NS +">\n" +
                "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "PREFIX ex: <"+EX+">\n";
        Lit[] i = new Lit[10];
        for (int j = 0; j < i.length; j++)
            i[j] = StdLit.fromEscaped(String.valueOf(j), xsdInteger);

        return Stream.of(
                asList(0, prolog+"SELECT * WHERE {ex:l1 ex:p1 ?x.}",
                       prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x; ex:p2 ?y.}", 0,
                       singletonList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build())),
                asList(0, prolog+"SELECT * WHERE {ex:l1 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r2 ex:p1 ?x ; ex:p2 ?y.}", 0,
                       asList(
                               MapSolution.builder().put(x, i[1]).put(y, i[5]).build(),
                               MapSolution.builder().put(x, i[1]).put(y, i[6]).build(),
                               MapSolution.builder().put(x, i[2]).put(y, i[5]).build(),
                               MapSolution.builder().put(x, i[2]).put(y, i[6]).build()
                       )),
                asList(0, prolog+"SELECT * WHERE {ex:l1 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r3 ex:p1 ?x ; ex:p2 ?y.}", 0, emptyList()),
                asList(0, prolog+"SELECT *  WHERE {ex:l4 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x; ex:p2 ?y}", 0,
                       singletonList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build())),
                // OPTIONAL on left has no effect
                asList(0, prolog+"SELECT *  WHERE {ex:l4 ex:p1 ?x}",
                        prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x; ex:p2 ?y}", 0x01,
                        singletonList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build())),
                // OPTIONAL on right has no effect
                asList(0, prolog+"SELECT *  WHERE {ex:l4 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x; ex:p2 ?y}", 0x02,
                       singletonList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build())),
                // OPTIONAL on both has no effect
                asList(0, prolog+"SELECT *  WHERE {ex:l4 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x; ex:p2 ?y}", 0x03,
                       singletonList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build())),
                // OPTIONAL on left has no effect
                asList(0, prolog+"SELECT * WHERE {ex:l1 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x ; ex:p2 ?y}", 0x01,
                       singletonList(MapSolution.builder().put(x, i[1]).put(y, i[3]).build())),
                //OPTIONAL on right introduces new solutions
                asList(0, prolog+"SELECT * WHERE {ex:l1 ex:p1 ?x}",
                       prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x ; ex:p2 ?y}", 0x02,
                       asList(
                               MapSolution.builder().put(x, i[1]).put(y, i[3]).build(),
                               MapSolution.builder().put(x, i[2]).put(y, null).build())),
                // reverse operands of the previous test
                asList(0, prolog+"SELECT * WHERE {ex:r1 ex:p1 ?x ; ex:p2 ?y}",
                        prolog+"SELECT * WHERE {ex:l1 ex:p1 ?x}", 0x01,
                        asList(
                                MapSolution.builder().put(x, i[1]).put(y, i[3]).build(),
                                MapSolution.builder().put(x, i[2]).put(y, null).build()))
        ).flatMap(row -> { //swap left & right
            if ((Integer) row.get(3) != 0) // do not generate reversed if OPTIONAL
                return Stream.of(row);
            ArrayList<Object> reverse = new ArrayList<>(row);
            Object tmp = reverse.get(1);
            reverse.set(1, reverse.get(2));
            reverse.set(2, tmp);
            return Stream.of(row, reverse);
        }).flatMap(row -> { // scenario MUST behave equally without VALUES support on endpoint
            if (!row.get(0).equals(0))
                return Stream.of(row);
            ArrayList<Object> noValues = new ArrayList<>(row);
            noValues.set(0, 1);
            return Stream.of(row, noValues);
        }).flatMap(row -> factories.stream()
                        .map(f -> {  // prepend a factory to each test case
                            if (((Integer)row.get(3) & 0x01) != 0 && !f.canLeftOptional())
                                return null; //skip if cannot handle left optionals
                            ArrayList<Object> copy = new ArrayList<>(row);
                            copy.add(0, f);
                            return copy;
                        }).filter(Objects::nonNull)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    public void doTest(@Nonnull JoinFactory f, int ep, @Nonnull String leftSparql,
                       @Nonnull String rightSparql, int optionalBitmap,
                       @Nonnull Collection<Solution> expected, int threads) throws Exception {
        SPARQLParser parser = SPARQLParser.strict();
        EndpointQueryOp l = new EndpointQueryOp(eps.get(ep), parser.parseConjunctive(leftSparql));
        EndpointQueryOp r = new EndpointQueryOp(eps.get(ep), parser.parseConjunctive(rightSparql));
        if ((optionalBitmap & 0x1) != 0)
            l.modifiers().add(Optional.EXPLICIT);
        if ((optionalBitmap & 0x2) != 0)
            r.modifiers().add(Optional.EXPLICIT);
        List<Future<?>> futures = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(() -> {
                try {
                    ResultsAssert.assertExpectedResults(f.apply(l, r), expected);
                } finally {
                    latch.countDown();
                }
            }));
        }
        latch.await();
        for (Future<?> future : futures)
            future.get(); //thrown failures
    }

    @Test(dataProvider = "testData", groups = {"fast"})
    public void testSingleThread(@Nonnull JoinFactory f, int ep, @Nonnull String leftSparql,
                     @Nonnull String rightSparql, int optionalBitmap,
                     @Nonnull Collection<Solution> expected) throws Exception {
        doTest(f, ep, leftSparql, rightSparql, optionalBitmap, expected, 1);
    }

    @Test(dataProvider = "testData")
    public void testMultiThread(@Nonnull JoinFactory f, int ep, @Nonnull String leftSparql,
                     @Nonnull String rightSparql, int optionalBitmap,
                     @Nonnull Collection<Solution> expected) throws Exception {
        doTest(f, ep, leftSparql, rightSparql, optionalBitmap, expected, 128);
    }
}
