package br.ufsc.lapesd.freqel.federation.decomp.agglutinator;

import br.ufsc.lapesd.freqel.BSBMSelfTest;
import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.semantic.SemanticSelectDescription;
import br.ufsc.lapesd.freqel.federation.SimpleFederationModule;
import br.ufsc.lapesd.freqel.federation.concurrent.PoolPlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.decomp.match.SourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.ThreadedPerformanceListener;
import br.ufsc.lapesd.freqel.federation.planner.PrePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.ConjunctivePlanBenchmarksTestBase;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.decorators.EndpointDecorators;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.reason.tbox.EmptyTBox;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import br.ufsc.lapesd.freqel.rel.cql.CassandraCQEndpoint;
import br.ufsc.lapesd.freqel.rel.sql.JDBCCQEndpoint;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.freqel.webapis.description.APIMolecule;
import br.ufsc.lapesd.freqel.webapis.description.APIMoleculeMatcher;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.webapis.requests.impl.UriTemplateExecutor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.rdf.model.ModelFactory;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.*;

public class AgglutinatorTest implements TestContext {
    private static final PoolPlanningExecutorService poolExecutorService
            = new PoolPlanningExecutorService();
    private static final ThreadedPerformanceListener performanceListener
            = new ThreadedPerformanceListener();

    private static final List<Supplier<Agglutinator>> agglutinatorSuppliers = asList(
            EvenAgglutinator::new,
            StandardAgglutinator::new,
            ParallelStandardAgglutinator::new,
            () -> new ParallelStandardAgglutinator(performanceListener, poolExecutorService)
    );

    @BeforeClass(groups = {"fast"})
    public void beforeClass() {
        poolExecutorService.bind();
    }

    @AfterClass(groups = {"fast"})
    public void afterClass() {
        poolExecutorService.release();
        performanceListener.close();
    }

    static class InterceptingAgglutinator implements Agglutinator {
        private @Nonnull final Agglutinator delegate;
        public @Nonnull Map<TPEndpoint, CQueryMatch> matches = new HashMap<>();

        public InterceptingAgglutinator(@Nonnull Agglutinator delegate) {
            this.delegate = delegate;
        }

        @Override public void setMatchingStrategy(@Nonnull MatchingStrategy strategy) {
            delegate.setMatchingStrategy(strategy);
        }

        @Override public @Nonnull State createState(@Nonnull CQuery query) {
            matches.clear();
            return new State(delegate.createState(query));
        }

        private class State implements Agglutinator.State {
            private @Nonnull final Agglutinator.State delegate;

            public State(@Nonnull Agglutinator.State delegate) {
                this.delegate = delegate;
            }

            @Override public void addMatch(@Nonnull TPEndpoint ep, @Nonnull CQueryMatch match) {
                CQueryMatch old = matches.put(ep, match);
                assert old == null;
                delegate.addMatch(ep, match);
            }

            @Override public @Nonnull Collection<Op> takeLeaves() {
                return delegate.takeLeaves();
            }
        }
    }

    private static @Nonnull List<CQuery>
    getAllQueries(@Nonnull List<String> filenames,
                  @Nonnull Function<String, Op> loader) {
        PrePlanner prePlanner = new SimpleFederationModule.DefaultPrePlannerProvider(
                new NoOpPerformanceListener()).get();

        List<CQuery> list = new ArrayList<>();
        for (String filename : filenames) {
            Op root = loader.apply(filename);
            root = prePlanner.plan(root);
            TreeUtils.streamPreOrder(root).filter(QueryOp.class::isInstance)
                    .map(o -> ((QueryOp)o).getQuery())
                    .filter(q -> q.getModifiers().optional() == null)
                    .forEach(list::add);
        }
        return list;
    }

    private static  @Nonnull List<CQEndpoint> toSemanticSources(@Nonnull List<TPEndpoint> sources) {
        List<CQEndpoint> list = new ArrayList<>();
        for (TPEndpoint ep : sources) {
            TBox reasoner = new EmptyTBox();
            CQEndpoint cep = (CQEndpoint) ep;
            SemanticSelectDescription description = new SemanticSelectDescription(cep, reasoner);
            list.add(EndpointDecorators.withDescription(cep, description));
        }
        return list;
    }

    @DataProvider
    public static @Nonnull Object[][] agglutinateBenchmarksData() throws Exception {
        List<CQuery> lrbQueries = getAllQueries(LargeRDFBenchSelfTest.QUERY_FILENAMES, n -> {
            try {
                return LargeRDFBenchSelfTest.loadQuery(n);
            } catch (IOException | SPARQLParseException e) {
                throw new RuntimeException(e);
            }
        });
        List<CQuery> bsbmQueries = getAllQueries(BSBMSelfTest.QUERY_FILENAMES, n -> {
            try {
                return BSBMSelfTest.loadQuery(n);
            } catch (IOException | SPARQLParseException e) {
                throw new RuntimeException(e);
            }
        });
        List<TPEndpoint> lrbSources = ConjunctivePlanBenchmarksTestBase.largeRDFBenchSources();
        List<TPEndpoint> bsbmSources = ConjunctivePlanBenchmarksTestBase.bsbmSources();
        List<CQEndpoint> semLRBSources = toSemanticSources(lrbSources);
        List<CQEndpoint> semBSBMSources = toSemanticSources(bsbmSources);

        List<Object[]> rows = new ArrayList<>();
        for (CQuery q : lrbQueries)
            rows.add(new Object[]{q, lrbSources});
        for (CQuery q : bsbmQueries)
            rows.add(new Object[]{q, bsbmSources});
        for (CQuery q : lrbQueries)
            rows.add(new Object[]{q, semLRBSources});
        for (CQuery q : bsbmQueries)
            rows.add(new Object[]{q, semBSBMSources});

        return agglutinatorSuppliers.stream().flatMap(agglutinator -> rows.stream().map(base -> {
            Object[] copy = Arrays.copyOf(base, base.length + 1);
            copy[base.length] = agglutinator.get();
            return copy;
        })).toArray(Object[][]::new);
    }

    @Test(dataProvider = "agglutinateBenchmarksData", groups = {"fast"})
    public void testAgglutinateBenchmarks(@Nonnull CQuery query, @Nonnull List<TPEndpoint> sources,
                                          @Nonnull Agglutinator agglutinator) {
        SourcesListMatchingStrategy matchingStrategy = new SourcesListMatchingStrategy();
        sources.forEach(matchingStrategy::addSource);
        agglutinator.setMatchingStrategy(matchingStrategy);
        for (int i = 0; i < 4; i++) {
            InterceptingAgglutinator intercepting = new InterceptingAgglutinator(agglutinator);
            Collection<Op> nodes = matchingStrategy.match(query, intercepting);
            checkValidAgglutination(nodes, query);
            checkLostComponents(intercepting.matches, nodes);
        }
    }

    @Test(dataProvider = "agglutinateBenchmarksData")
    public void testConcurrentAgglutinateBenchmarks(@Nonnull CQuery query,
                                                    @Nonnull List<CQEndpoint> sources,
                                                    @Nonnull Agglutinator agglutinator)
            throws ExecutionException, InterruptedException {
        SourcesListMatchingStrategy matchingStrategy = new SourcesListMatchingStrategy();
        sources.forEach(matchingStrategy::addSource);
        agglutinator.setMatchingStrategy(matchingStrategy);
        ExecutorService exec = Executors.newCachedThreadPool();
        List<Future<ImmutablePair<Collection<Op>, Map<TPEndpoint, CQueryMatch>>>> futures;
        futures = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 10; i++) {
            futures.add(exec.submit(() -> {
                InterceptingAgglutinator intercepting = new InterceptingAgglutinator(agglutinator);
                Collection<Op> nodes = matchingStrategy.match(query, intercepting);
                return ImmutablePair.of(nodes, intercepting.matches);
            }));
        }
        for (Future<ImmutablePair<Collection<Op>, Map<TPEndpoint, CQueryMatch>>> f : futures) {
            ImmutablePair<Collection<Op>, Map<TPEndpoint, CQueryMatch>> p = f.get();
            checkValidAgglutination(p.left, query);
            checkLostComponents(p.right, p.left);
        }
    }

    @Test(enabled = false)
    public static void checkLostComponents(@Nonnull Map<TPEndpoint, CQueryMatch> matches,
                                     @Nonnull Collection<Op> nodes) {

        for (Map.Entry<TPEndpoint, CQueryMatch> e : matches.entrySet()) {
            TPEndpoint ep = e.getKey();
            List<EndpointQueryOp> epNodes = nodes.stream()
                    .flatMap(n -> n instanceof EndpointQueryOp
                            ? Stream.of((EndpointQueryOp) n)
                            : n.getChildren().stream().map(c -> (EndpointQueryOp) c))
                    .filter(n -> n.getEndpoint() == ep).collect(toList());

            // every exclusive group (or triple) in a match appears at least once in the nodes
            for (CQuery eg : e.getValue().getKnownExclusiveGroups())
                assertTrue(epNodes.stream().anyMatch(n -> isContained(eg, n.getQuery())));
            for (Triple triple : e.getValue().getNonExclusiveRelevant()) {
                CQuery q = CQuery.from(triple);
                assertTrue(epNodes.stream().anyMatch(n -> isContained(q, n.getQuery())));
            }
        }
    }

    public static boolean isContained(@Nonnull CQuery subQuery, @Nonnull CQuery superQuery) {
        if (!superQuery.containsAll(subQuery)) return false;
        boolean[] ok = {true};
        subQuery.forEachTermAnnotation((t, a) -> {
            if (!superQuery.getTermAnnotations(t).contains(a))
                ok[0] = false;
        });
        subQuery.forEachTripleAnnotation((t, a) -> {
            if (!superQuery.getTripleAnnotations(t).contains(a))
                ok[0] = false;
        });
        return ok[0];
    }

    @Test(enabled = false)
    public static void checkValidAgglutination(@Nonnull Collection<Op> nodes,
                                               @Nonnull CQuery query) {
        Set<Triple> matched = new HashSet<>();
        IdentityHashMap<TPEndpoint, List<EndpointQueryOp>> ep2nodes = new IdentityHashMap<>();
        for (Op node : nodes) {
            if (node instanceof UnionOp) {
                assertTrue(node.getChildren().stream().allMatch(EndpointQueryOp.class::isInstance));
                for (Op child : node.getChildren()) {
                    EndpointQueryOp qo = ((EndpointQueryOp) child);
                    ep2nodes.computeIfAbsent(qo.getEndpoint(), k -> new ArrayList<>()).add(qo);
                }
                // all children of an union must share the same interface
                assertFalse(node.getChildren().isEmpty());
                Op first = node.getChildren().iterator().next();
                Set<Triple> matchedTriples = first.getMatchedTriples();
                assertFalse(matchedTriples.isEmpty());
                Set<String> inputVars = first.getInputVars();
                for (Op child : node.getChildren()) {
                    assertEquals(child.getMatchedTriples(), matchedTriples);
                    assertEquals(child.getInputVars(), inputVars);
                }
                assertTrue(node.getChildren().size() > 1);
            } else if (node instanceof EndpointQueryOp) {
                EndpointQueryOp qo = (EndpointQueryOp) node;
                ep2nodes.computeIfAbsent(qo.getEndpoint(), k -> new ArrayList<>()).add(qo);
            } else {
                fail("Unexpected node class: "+node.getClass().getSimpleName());
            }
        }

        for (Map.Entry<TPEndpoint, List<EndpointQueryOp>> e : ep2nodes.entrySet()) {
            TPEndpoint ep = e.getKey();
            assertEquals(new HashSet<>(e.getValue()).size(), e.getValue().size(),
                         "duplicates for ep"+ ep);
            assertTrue(e.getValue().stream().noneMatch(o -> o.getQuery().isEmpty()),
                       "There are empty queries");
            if (e.getValue().stream().anyMatch(o -> o.getQuery().size() > 1))
                assertTrue(ep instanceof CQEndpoint);
            if (ep instanceof WebAPICQEndpoint ||ep instanceof JDBCCQEndpoint
                    || ep instanceof CassandraCQEndpoint ) {
                for (EndpointQueryOp op : e.getValue())
                    assertTrue(op.getQuery().hasTermAnnotations(AtomAnnotation.class));
            }
        }

        ep2nodes.values().stream().flatMap(List::stream)
                .forEach(o -> matched.addAll(o.getQuery().attr().matchedTriples()));
        assertEquals(matched, query.attr().getSet());
        assertEquals(matched, query.attr().matchedTriples());
    }

    @DataProvider @Nonnull Object[][] testInputsPathData() {
        return Stream.of(2, 4, 8)
                .flatMap(size -> Stream.of(1, 2)
                        .flatMap(sources -> agglutinatorSuppliers.stream()
                                .map(agg -> new Object[] {agg.get(), size, sources}))
                ).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testInputsPathData", groups = {"fast"})
    public void testInputsPath(@Nonnull Agglutinator agglutinator, int size, int sources) {
        List<Atom> atoms = range(1, size+1).mapToObj(i -> new Atom("A"+i)).collect(toList());
        SourcesListMatchingStrategy matchingStrategy = new SourcesListMatchingStrategy();
        MutableCQuery query = new MutableCQuery();
        for (int i = 0; i < size; i++) {
            Term s = i == 0 ? Alice : new StdVar("x"+i);
            Term p = new StdURI(EX+"p"+i);
            Term o = i == size-1 ? Bob : new StdVar("x"+(i+1));
            query.add(new Triple(s, p, o));
            for (int j = 0; j < sources; j++) {
                ARQEndpoint ep = ARQEndpoint.forModel(ModelFactory.createDefaultModel());
                Molecule molecule = Molecule.builder("Mol-" + i+"-"+j)
                        .out(p, atoms.get(i)).exclusive().build();
                String inputName = format("in-%d-%d", i, j);
                Map<String, String> el2in = new HashMap<>();
                el2in.put(atoms.get(i).getName(), inputName);
                UriTemplate tpl = new UriTemplate(format("%s%d-%d{?%s}", EX, i, j, inputName));
                UriTemplateExecutor uriExecutor =
                        UriTemplateExecutor.from(tpl).withRequired(inputName).build();
                APIMolecule apiMolecule = new APIMolecule(molecule, uriExecutor, el2in);
                APIMoleculeMatcher matcher = new APIMoleculeMatcher(apiMolecule);
                matchingStrategy.addSource(EndpointDecorators.withDescription(ep, matcher));
            }
        }
        agglutinator.setMatchingStrategy(matchingStrategy);
        InterceptingAgglutinator intercepting = new InterceptingAgglutinator(agglutinator);
        Collection<Op> nodes = matchingStrategy.match(query, intercepting);
        assertTrue(nodes.size() >= size);
        assertTrue(nodes.size() <= size*sources);
        if (agglutinator instanceof EvenAgglutinator)
            assertEquals(nodes.size(), size*sources);
        for (Op node : nodes) {
            if (node instanceof EndpointQueryOp) {
                MutableCQuery q = ((EndpointQueryOp) node).getQuery();
                assertTrue(query.containsAll(q));
                assertEquals(q.size(), 1);
                Term object = q.get(0).getObject();
                if (object.isVar()) {
                    int i = Integer.parseInt(object.asVar().getName().replace("x", ""));
                    List<AtomInputAnnotation> anns = q.getTermAnnotations(object).stream()
                            .filter(AtomInputAnnotation.class::isInstance)
                            .map(a -> (AtomInputAnnotation)a)
                            .collect(toList());
                    assertEquals(anns.size(), 1);
                    assertTrue(anns.get(0).isRequired());
                    String inputName = anns.get(0).getInputName();
                    assertTrue(inputName.matches("in-"+(i-1)+"-\\d+"), "inputName="+inputName);
                }
            } else {
                List<Integer> observedSources = new ArrayList<>();
                assertTrue(node instanceof UnionOp);
                boolean hasObject = false;
                for (Op child : node.getChildren()) {
                    assertTrue(child instanceof EndpointQueryOp);
                    MutableCQuery q = ((EndpointQueryOp) child).getQuery();
                    assertTrue(query.containsAll(q));
                    assertEquals(q.size(), 1);
                    Term object = q.get(0).getObject();
                    if (object.isVar()) {
                        hasObject = true;
                        int i = Integer.parseInt(object.asVar().getName().replace("x", ""));
                        List<AtomInputAnnotation> anns = q.getTermAnnotations(object).stream()
                                .filter(AtomInputAnnotation.class::isInstance)
                                .map(a -> (AtomInputAnnotation)a)
                                .collect(toList());
                        assertEquals(anns.size(), 1);
                        assertTrue(anns.get(0).isRequired());
                        String inputName = anns.get(0).getInputName();
                        Matcher matcher = Pattern.compile("in-" + (i-1) + "-(\\d+)").matcher(inputName);
                        assertTrue(matcher.matches(), "inputName="+inputName);
                        observedSources.add(Integer.parseInt(matcher.group(1)));
                    }
                }
                if (hasObject) {
                    assertEquals(observedSources.size(), sources);
                    assertEquals(new HashSet<>(observedSources),
                            IntStream.range(0, sources).boxed().collect(toSet()));
                }
            }
        }
        checkValidAgglutination(nodes, query);
        checkLostComponents(intercepting.matches, nodes);
    }
}