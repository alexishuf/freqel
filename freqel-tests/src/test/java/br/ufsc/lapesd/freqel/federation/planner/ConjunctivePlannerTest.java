package br.ufsc.lapesd.freqel.federation.planner;

import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.TestComponent;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.JoinPathsConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerDispatcher;
import br.ufsc.lapesd.freqel.federation.planner.equiv.DefaultEquivCleaner;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.webapis.TransparencyService;
import br.ufsc.lapesd.freqel.webapis.TransparencyServiceTestContext;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.PlanAssert.assertPlanAnswers;
import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.*;

public class ConjunctivePlannerTest implements TransparencyServiceTestContext {
    public static final @Nonnull StdLit title1 = StdLit.fromEscaped("title 1", "en");
    public static final @Nonnull StdLit author1 = StdLit.fromUnescaped("author 1", "en");

    private static final URI o3 = new StdURI("http://example.org/o3");
    private static final Var a = new StdVar("a");
    private static final Var b = new StdVar("b");
    private static final Var c = new StdVar("c");
    private static final Var d = new StdVar("d");
    private static final Var s = new StdVar("s");
    private static final Var t = new StdVar("t");

    public static @Nonnull List<Class<? extends ConjunctivePlanner>> plannerClasses = asList(
                    JoinPathsConjunctivePlanner.class,
                    // diverts to BitsetConjunctivePlanner and BitsetNoInputsConjunctivePlanner
                    BitsetConjunctivePlannerDispatcher.class,
                    // handles inputs but should also work without inputs fails if missing universes
                    BitsetConjunctivePlanner.class
    );
    public static @Nonnull List<Class<? extends JoinOrderPlanner>> joinOrderPlannerClasses
            = asList(ArbitraryJoinOrderPlanner.class, GreedyJoinOrderPlanner.class);

    public static boolean isFast(@Nonnull Class<? extends ConjunctivePlanner> planner,
                                 @Nonnull Class<? extends JoinOrderPlanner> joinPlanner) {
        return !joinPlanner.equals(ArbitraryJoinOrderPlanner.class);
    }

    public static @Nonnull List<Supplier<ConjunctivePlanner>> suppliers;

    static {
        suppliers = new ArrayList<>();
        for (Class<? extends ConjunctivePlanner> p : plannerClasses) {
            for (Class<? extends JoinOrderPlanner> op : joinOrderPlannerClasses) {
                Supplier<ConjunctivePlanner> supplier = () -> {
                    FreqelConfig config = FreqelConfig.createDefault();
                    config.set(FreqelConfig.Key.CONJUNCTIVE_PLANNER, p);
                    if (p.equals(JoinPathsConjunctivePlanner.class) && op.equals(GreedyJoinOrderPlanner.class))
                        config.set(FreqelConfig.Key.EQUIV_CLEANER, DefaultEquivCleaner.class);
                    config.set(FreqelConfig.Key.JOIN_ORDER_PLANNER, op);
                    TestComponent.Builder b = DaggerTestComponent.builder();
                    b.overrideFreqelConfig(config);
                    return b.build().conjunctivePlanner();
                };
                String name = p.getSimpleName() + "+" + op.getSimpleName();
                suppliers.add(new NamedSupplier<>(name, supplier));
            }
        }
    }

    private static final @Nonnull
    EmptyEndpoint empty1  = new EmptyEndpoint(),  empty2 = new EmptyEndpoint(),
                  empty3a = new EmptyEndpoint(), empty3b = new EmptyEndpoint(),
                  empty4  = new EmptyEndpoint();


    public static final @Nonnull Atom Book = new Atom("Book");
    public static final @Nonnull Atom Person = new Atom("Person");
    public static final @Nonnull Atom KnownPerson = new Atom("KnownPerson");

    static {
        empty3a.addAlternative(empty3b);
        empty3b.addAlternative(empty3a);
    }

    private static @Nonnull Op plan(@Nonnull ConjunctivePlanner p,
                                    @Nonnull CQuery q, @Nonnull Collection<Op> nodes) {
        if (p instanceof BitsetConjunctivePlannerDispatcher
                || p instanceof BitsetConjunctivePlanner) {
            IndexSet<Triple> triples = FullIndexSet.fromDistinct(Stream.concat(
                    Stream.of(q),
                    nodes.stream().flatMap(n -> streamPreOrder(n).filter(QueryOp.class::isInstance)
                                  .map(o4 -> ((QueryOp) o4).getQuery()))
            ).flatMap(Collection::stream).collect(toSet()));
            IndexSet<String> vars = FullIndexSet.fromDistinct(Stream.concat(
                    q.attr().allVarNames().stream(),
                    nodes.stream().flatMap(
                            r -> streamPreOrder(r).flatMap(n -> n.getAllVars().stream()))
            ).collect(toSet()));
            for (Op root : nodes) {
                streamPreOrder(root).forEach(n -> {
                    n.offerTriplesUniverse(triples);
                    n.offerVarsUniverse(vars);
                    n.purgeCaches();
                });
            }
            q.attr().offerTriplesUniverse(triples);
            q.attr().offerVarNamesUniverse(vars);
        }
        return p.plan(q, nodes);
    }


    @DataProvider
    public static Object[][] suppliersData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testSingleQuery(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = createQuery(Alice, knows, x);
        EndpointQueryOp queryOp = new EndpointQueryOp(empty1, query);
        Op node = plan(planner, query, singleton(queryOp));
        assertSame(node, queryOp);
        assertPlanAnswers(node, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testDuplicateQuery(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = createQuery(Alice, knows, x);
        EndpointQueryOp node1 = new EndpointQueryOp(empty1, query);
        EndpointQueryOp node2 = new EndpointQueryOp(empty2, query);
        Op root = plan(planner, query, asList(node1, node2));
        assertEquals(root.getResultVars(), singleton("x"));
        assertFalse(root.isProjected());

        // no good reason for more than 3 nodes
        assertTrue(streamPreOrder(root).count() <= 3);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testSingleJoin(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(Alice, knows, x),
                                   new Triple(x, knows, y));
        CQuery q1 = CQuery.from(query.get(0));
        CQuery q2 = CQuery.from(query.get(1));
        EndpointQueryOp node1 = new EndpointQueryOp(empty1, q1);
        EndpointQueryOp node2 = new EndpointQueryOp(empty1, q2);
        Op root = plan(planner, query, asList(node1, node2));
        assertEquals(root.getResultVars(), asList("x", "y"));

        // a reasonable plan would just add a join node over the query nodes
        assertTrue(streamPreOrder(root).count() <= 5);
        List<JoinOp> joins = streamPreOrder(root).filter(n -> n instanceof JoinOp)
                                         .map(n -> (JoinOp)n).collect(toList());
        assertEquals(joins.size(), 1);
        assertEquals(joins.get(0).getJoinVars(), singleton("x"));

        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testDoNotJoinSameVarsDifferentQueries(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = CQuery.from(
                new Triple(x, knows, Alice), new Triple(x, knows, y), new Triple(x, manages, y)
        );
        CQuery q1 = CQuery.from(query.get(0), query.get(1));
        CQuery q2 = CQuery.from(query.get(2));
        Op root = plan(planner, query, asList(new EndpointQueryOp(empty1, q1),
                                                   new EndpointQueryOp(empty2, q2)));

        assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinOp).count(), 1);
        assertEquals(streamPreOrder(root)
                              .filter(n -> n instanceof UnionOp).count(), 0);
        // a sane count is 3: MultiQuery(q1, q2)
        assertTrue(streamPreOrder(root).count() <= 4);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testCartesianProduct(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        if (!(planner instanceof JoinPathsConjunctivePlanner))
            return; //splitting cartesians is not mandatory for implementations
        CQuery query = CQuery.from(new Triple(Alice, knows, x), new Triple(y, knows, Bob));
        CQuery q1 = CQuery.from(query.get(0));
        CQuery q2 = CQuery.from(query.get(1));
        EndpointQueryOp node1 = new EndpointQueryOp(empty1, q1), node2 = new EndpointQueryOp(empty1, q2);
        Op root = plan(planner, query, asList(node1, node2));
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));

        assertTrue(streamPreOrder(root).count() <= 5);
        List<CartesianOp> nodes = streamPreOrder(root)
                                             .filter(n -> n instanceof CartesianOp)
                                             .map(n -> (CartesianOp) n).collect(toList());
        assertEquals(nodes.size(), 1);
        assertEquals(nodes.get(0).getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(nodes.get(0).getChildren().size(), 2);

        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testLargeTree(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        if (!(supplier.get() instanceof JoinPathsConjunctivePlanner))
            return; //siletly skip, since allowing disconnected queries is not mandatory
        CQuery query = CQuery.from(
                new Triple(Alice, knows, x),
                new Triple(x, knows, y),
                new Triple(Alice, knows, u),
                new Triple(u, knows, v)
        );
        CQuery q1 = CQuery.from(query.get(0));
        CQuery q2 = CQuery.from(query.get(1));
        CQuery q3 = CQuery.from(query.get(2));
        CQuery q4 = CQuery.from(query.get(3));

        List<EndpointQueryOp> leaves = asList(new EndpointQueryOp(empty1, q1), new EndpointQueryOp(empty2, q1),
                                        new EndpointQueryOp(empty1, q2),
                                        new EndpointQueryOp(empty1, q3),
                                        new EndpointQueryOp(empty1, q4), new EndpointQueryOp(empty2, q4));
        Random random = new Random(79812531);
        for (int i = 0; i < 16; i++) {
            ArrayList<Op> shuffled = new ArrayList<>(leaves);
            Collections.shuffle(shuffled, random);
            Op root = plan(planner, query, shuffled);

            Set<EndpointQueryOp> observed = streamPreOrder(root)
                                               .filter(n -> n instanceof EndpointQueryOp)
                                               .map(n -> (EndpointQueryOp) n).collect(toSet());
            assertEquals(observed, new HashSet<>(shuffled));
            List<JoinOp> joinOps = streamPreOrder(root)
                                                .filter(n -> n instanceof JoinOp)
                                                .map(n -> (JoinOp) n).collect(toList());
            assertEquals(joinOps.size(), 2);

            Set<Set<String>> actualSets = joinOps.stream().map(Op::getResultVars)
                                                            .collect(toSet());
            Set<Set<String>> exSets = new HashSet<>();
            exSets.add(Sets.newHashSet("x", "y"));
            exSets.add(Sets.newHashSet("u", "v"));
            assertEquals(actualSets, exSets);

            actualSets = joinOps.stream().map(JoinOp::getJoinVars).collect(toSet());
            exSets.clear();
            exSets.add(singleton("x"));
            exSets.add(singleton("u"));
            assertEquals(actualSets, exSets);

            List<CartesianOp> cartesianOps = streamPreOrder(root)
                                                          .filter(n -> n instanceof CartesianOp)
                                                          .map(n -> (CartesianOp) n)
                                                          .collect(toList());
            assertEquals(cartesianOps.size(), 1);
            assertEquals(cartesianOps.get(0).getResultVars(),
                         Sets.newHashSet("x", "y", "u", "v"));
            assertEquals(cartesianOps.get(0).getChildren().size(), 2);

            assertPlanAnswers(root, query);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testBookShop(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();

        CQuery query = CQuery.from(new Triple(x, title, title1),
                                   new Triple(x, genre, y),
                                   new Triple(y, genreName, z));
        EndpointQueryOp q1 = new EndpointQueryOp(empty1, CQuery.from(query.get(0)));
        EndpointQueryOp q2 = new EndpointQueryOp(empty1, CQuery.from(query.get(1)));
        EndpointQueryOp q3 = new EndpointQueryOp(empty2, CQuery.from(query.get(2)));
        Op root = plan(planner, query, asList(q1, q2, q3));

        assertEquals(streamPreOrder(root).filter(n -> n instanceof CartesianOp).count(), 0);
        assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinOp).count(), 2);

        Set<CQuery> queries = streamPreOrder(root).filter(n -> n instanceof EndpointQueryOp)
                .map(n -> ((EndpointQueryOp) n).getQuery()).collect(toSet());
        HashSet<CQuery> expectedQueries = Sets.newHashSet(
                createQuery(x, title, title1),
                createQuery(x, genre, y),
                createQuery(y, genreName, z)
        );
        assertEquals(queries, expectedQueries);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testSameQueryEquivalentEp(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = createQuery(x, knows, y, y, knows, z);
        EndpointQueryOp q1 = new EndpointQueryOp(empty3a, CQuery.from(query.get(0)));
        EndpointQueryOp q2 = new EndpointQueryOp(empty3b, CQuery.from(query.get(0)));
        EndpointQueryOp q3 = new EndpointQueryOp(empty1 , CQuery.from(query.get(1)));

        IndexSet<String> vars = FullIndexSet.newIndexSet("x", "y", "z");
        IndexSet<Triple> triples = FullIndexSet.fromDistinct(query);
        Stream.concat(Stream.of(query), Stream.of(q1, q2, q3).map(QueryOp::getQuery)).forEach(q -> {
            q.attr().offerVarNamesUniverse(vars);
            q.attr().offerTriplesUniverse(triples);
        });

        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(asList((Op)q1, q2, q3))) {
            Op plan = plan(planner, query, permutation);
            Set<Op> qns = streamPreOrder(plan).filter(EndpointQueryOp.class::isInstance).collect(toSet());
            assertTrue(qns.contains(q3));
            assertEquals(qns.size(), 2);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void booksByAuthorWithIncompatibleService(Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        EndpointQueryOp q1 = new EndpointQueryOp(empty1, createQuery(y, name, author1));
        EndpointQueryOp q2 = new EndpointQueryOp(empty2,
                createQuery(x, AtomInputAnnotation.asRequired(Book, "Book").get(),
                                author, y, AtomAnnotation.of(Person)));
        CQuery query = CQuery.from(new Triple(y, name, author1), new Triple(x, author, y));

        for (List<Op> nodes : asList(asList((Op)q1, q2), asList((Op)q2, q1))) {
            Op plan = plan(planner, query, nodes);
            assertTrue(plan instanceof EmptyOp);
            assertEquals(plan.getResultVars(), Sets.newHashSet("x", "y"));
            assertEquals(plan.getRequiredInputVars(), emptySet());
            assertPlanAnswers(plan, query, true, true);
        }
    }

    public void booksByAuthorWithServiceTest(@Nonnull ConjunctivePlanner planner, boolean markAsAlternatives,
                                             boolean addFromSubject) {
        CQEndpoint e2 = markAsAlternatives ? empty3a : empty2;
        CQEndpoint e3 = markAsAlternatives ? empty3b : empty4;

        EndpointQueryOp q1 = new EndpointQueryOp(empty1, createQuery(y, name, author1));
        assertEquals(q1.getStrictResultVars(), singleton("y"));
        EndpointQueryOp q2 = new EndpointQueryOp(e2,
                createQuery(x, AtomInputAnnotation.asRequired(Book, "Book").get(),
                                author, y, AtomAnnotation.of(Person)));
        EndpointQueryOp q3 = new EndpointQueryOp(e3,
                createQuery(x, AtomAnnotation.of(Book),
                                author, y, AtomInputAnnotation.asRequired(Person, "Person").get()));
        List<Op> nodes = addFromSubject ? asList(q1, q2, q3) : asList(q1, q3);
        CQuery query = CQuery.from(new Triple(y, name, author1),
                                   new Triple(x, author, y));

        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(nodes)) {
            Op root = plan(planner, query, permutation);
            Set<Op> leaves = streamPreOrder(root)
                    .filter(n -> n instanceof EndpointQueryOp).collect(toSet());
            assertEquals(leaves, Sets.newHashSet(q1, q3));
            assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinOp).count(), 1);
            assertTrue(streamPreOrder(root).count() <= 4);
            assertPlanAnswers(root, query);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testBooksByAuthorARQWithService(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        booksByAuthorWithServiceTest(supplier.get(), false, false);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testBooksByAuthorAlternativeService(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        booksByAuthorWithServiceTest(supplier.get(), true, true);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testBooksByAuthorLeftoverService(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        booksByAuthorWithServiceTest(supplier.get(), false, true);
    }

    private static class TwoServicePathsNodes {
        EndpointQueryOp q1, q2, q3, p1, p2, p3;
        List<Op> all, fromAlice, fromBob, fromAlice2, fromBob2;
        Set<Set<Op>> allowedAnswers;
        CQuery query;

        public TwoServicePathsNodes(@Nonnull CQEndpoint epFromAlice,
                                    @Nonnull CQEndpoint epFromBob) {
            query = CQuery.from(new Triple(Alice, knows, x),
                                new Triple(x, knows, y),
                                new Triple(y, knows, Bob));
            q1 = new EndpointQueryOp(epFromAlice, createQuery(
                    Alice, AtomInputAnnotation.asRequired(Person, "Person").get(),
                            knows, x, AtomAnnotation.of(KnownPerson)));
            q2 = new EndpointQueryOp(epFromAlice, createQuery(
                    x, AtomInputAnnotation.asRequired(Person, "Person").get(),
                            knows, y, AtomAnnotation.of(KnownPerson)));
            q3 = new EndpointQueryOp(epFromAlice, createQuery(y, AtomInputAnnotation.asRequired(Person, "Person").get(),
                            knows, Bob, AtomAnnotation.of(KnownPerson)));

            p1 = new EndpointQueryOp(epFromBob, createQuery(Alice, AtomAnnotation.of(Person),
                            knows, x, AtomInputAnnotation.asRequired(KnownPerson, "KnownPerson").get()));
            p2 = new EndpointQueryOp(epFromBob, createQuery(x, AtomAnnotation.of(Person),
                            knows, y, AtomInputAnnotation.asRequired(KnownPerson, "KnownPerson").get()));
            p3 = new EndpointQueryOp(epFromBob, createQuery(y, AtomAnnotation.of(Person), knows, Bob,
                    AtomInputAnnotation.asRequired(KnownPerson, "KnownPerson").get()));
            all = asList(q1, q2, q3, p1, p2, p3);
            fromAlice  = asList(q1, q2, q3);
            fromAlice2 = asList(q1, q2, p3);
            fromBob  = asList(p1, p2, p3);
            fromBob2 = asList(q1, p2, p3);
            allowedAnswers = new HashSet<>();
            allowedAnswers.add(new HashSet<>(fromAlice));
            allowedAnswers.add(new HashSet<>(fromAlice2));
            allowedAnswers.add(new HashSet<>(fromBob));
            allowedAnswers.add(new HashSet<>(fromBob2));
        }
    }

    private void onePathTwoDirectionsWithServicesTest(@Nonnull ConjunctivePlanner planner,
                                                      boolean useAlternative) {
        EmptyEndpoint ep1 = useAlternative ? empty3a : empty1;
        EmptyEndpoint ep2 = useAlternative ? empty3b : empty1;
        TwoServicePathsNodes f = new TwoServicePathsNodes(ep1, ep2);

        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(f.all)) {
            Op plan = plan(planner, f.query, permutation);
            Set<EndpointQueryOp> qns = streamPreOrder(plan).filter(EndpointQueryOp.class::isInstance)
                    .map(n -> (EndpointQueryOp)n).collect(toSet());
            assertTrue(f.allowedAnswers.contains(qns), "qns="+qns);
            assertEquals(streamPreOrder(plan).filter(JoinOp.class::isInstance).count(), 2);
            assertPlanAnswers(plan, f.query);
        }
    }

    @Test(dataProvider = "suppliersData", invocationCount = 4)
    public void testOnePathTwoDirectionsWithServicesSameEp(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        onePathTwoDirectionsWithServicesTest(supplier.get(), false);
    }

    @Test(dataProvider = "suppliersData", invocationCount = 4)
    public void testOnePathTwoDirectionsWithServicesAlternativeEp(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        onePathTwoDirectionsWithServicesTest(supplier.get(), true);
    }

    @Test(dataProvider = "suppliersData", invocationCount = 4)
    public void testOnePathTwoDirectionsUnrelatedEndpoints(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        TwoServicePathsNodes f = new TwoServicePathsNodes(empty1, empty2);
        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(f.all)) {
            Op plan = plan(planner, f.query, permutation);
            assertEquals(streamPreOrder(plan).filter(EndpointQueryOp.class::isInstance).collect(toSet()),
                         new HashSet<>(f.all));
            assertTrue(streamPreOrder(plan).filter(n -> n == f.q1).count() <= 3);
            assertTrue(streamPreOrder(plan).filter(n -> n == f.p1).count() <= 1);
            assertTrue(streamPreOrder(plan).filter(n -> n == f.p3).count() <= 3);
            assertTrue(streamPreOrder(plan).filter(n -> n == f.q3).count() <= 1);
            assertPlanAnswers(plan, f.query);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testDiscardMiddleNodeInPath(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        TwoServicePathsNodes f = new TwoServicePathsNodes(empty1, empty2);
        List<Op> nodes = asList(f.q1, f.q2, f.q3, f.p2);
        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(nodes)) {
            Op plan = plan(planner, f.query, permutation);
            assertEquals(streamPreOrder(plan).filter(EndpointQueryOp.class::isInstance).collect(toSet()),
                         Sets.newHashSet(f.fromAlice));
            assertEquals(streamPreOrder(plan).filter(EndpointQueryOp.class::isInstance).count(), 3);
            assertPlanAnswers(plan, f.query);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testUnsatisfiablePlan(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        TwoServicePathsNodes f = new TwoServicePathsNodes(empty1, empty1);
        List<Op> nodes = asList(f.q1, f.p2, f.q3);
        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(nodes)) {
            Op plan = plan(planner, f.query, permutation);
            assertTrue(plan instanceof EmptyOp);
            assertEquals(plan.getRequiredInputVars(), emptySet());
            assertEquals(plan.getResultVars(), Sets.newHashSet("x", "y"));
            assertPlanAnswers(plan, f.query, true, true);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testNonLinearPath(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        EndpointQueryOp orgByDesc = new EndpointQueryOp(empty1, CQuery.from(
                new Triple(x, p1, t)
        ));
        EndpointQueryOp contract = new EndpointQueryOp(empty1, createQuery(
                y, p2, b,
                y, p3, c,
                y, p4, t, AtomInputAnnotation.asRequired(new Atom("A1"), "A1").get()));
        EndpointQueryOp contractById = new EndpointQueryOp(empty1, createQuery(
                b, AtomInputAnnotation.asRequired(new Atom("A2"), "A2").get(), p5, o3));
        EndpointQueryOp contractorByName = new EndpointQueryOp(empty1, createQuery(
                c, AtomInputAnnotation.asRequired(new Atom("A3"), "A3").get(), p6, s));
        EndpointQueryOp procurementsOfContractor = new EndpointQueryOp(empty1, createQuery(
                s, AtomInputAnnotation.asRequired(new Atom("A4"), "A4").get(), p7, a));
        EndpointQueryOp procurementById = new EndpointQueryOp(empty1, createQuery(
                a, AtomInputAnnotation.asRequired(new Atom("A5"), "A5").get(), p8, d));
        EndpointQueryOp modalities = new EndpointQueryOp(empty1, createQuery(z, p9, d));

        CQuery query = createQuery(
                x, p1, t,
                y, p2, b,
                y, p3, c,
                y, p4, t,
                b, p5, o3,
                c, p6, s,
                s, p7, a,
                a, p8, d,
                z, p9, d
        );

        ConjunctivePlanner planner = supplier.get();
        List<Op> nodes = asList(contractorByName, procurementsOfContractor,
                contractById, modalities, procurementById, orgByDesc, contract);
        assertTrue(nodes.stream().allMatch(n -> query.attr().getSet().containsAll(n.getMatchedTriples())));

        Op plan = plan(planner, query, nodes);
        assertPlanAnswers(plan, query);
    }

    @DataProvider
    public static Object[][] optionalQueriesData() {
        List<Object[]> list = new ArrayList<>();
        for (Object[] row : suppliersData()) {
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, JenaSPARQLFilter.build("?v <= 23"),
                    y, id, z
            )});
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, JenaSPARQLFilter.build("?v <= 23"),
                    y, id, z,
                    y, dataAbertura, w //filter optionals, atom is used as output
            )});
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, JenaSPARQLFilter.build("?v <= 23"),
                    y, id, z,
                    y, unidadeGestora, y1,
                    y1, orgaoVinculado, y2,
                    y2, codigoSIAFI, y3 // optional being used as output
            )});
        }
        return list.toArray(new Object[0][]);
    }

    @Test(dataProvider = "optionalQueriesData", groups={"fast"})
    public void testJoinArqWithOptionalInputs(@Nonnull Supplier<ConjunctivePlanner> supplier,
                                              @Nonnull CQuery webQuery) throws IOException {
        Model model = ModelFactory.createDefaultModel();
        model.add(createResource(EX+"Dummy"), createProperty(EX+"p1"), createTypedLiteral(20000));
        ARQEndpoint arqEp = ARQEndpoint.forModel(model);

        WebTarget fakeTarget = ClientBuilder.newClient().target("https://localhost:22/");
        WebAPICQEndpoint webEp = TransparencyService.getProcurementsOptClient(fakeTarget);

        CQuery arqQuery = createQuery(x, p1, v);
        EndpointQueryOp n1 = new EndpointQueryOp(arqEp, arqQuery);
        EndpointQueryOp n2 = new EndpointQueryOp(webEp, webQuery);

        JoinInfo info = JoinInfo.getJoinability(n1, n2);
        assertTrue(info.isValid());

        ConjunctivePlanner planner = supplier.get();
        CQuery wholeQuery = CQuery.merge(arqQuery, webQuery);
        Op plan = plan(planner, wholeQuery, asList(n1, n2));
        assertPlanAnswers(plan, wholeQuery, false, true);
    }
}