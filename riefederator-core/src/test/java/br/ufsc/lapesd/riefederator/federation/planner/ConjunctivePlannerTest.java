package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.JoinPathsConjunctivePlanner;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import br.ufsc.lapesd.riefederator.webapis.TransparencyService;
import br.ufsc.lapesd.riefederator.webapis.TransparencyServiceTestContext;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.isAcyclic;
import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
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

    public static @Nonnull List<Class<? extends ConjunctivePlanner>> plannerClasses
            = singletonList(JoinPathsConjunctivePlanner.class);
    public static @Nonnull List<Class<? extends JoinOrderPlanner>> joinOrderPlannerClasses
            = asList(ArbitraryJoinOrderPlanner.class, GreedyJoinOrderPlanner.class);

    public static boolean isFast(@Nonnull Class<? extends ConjunctivePlanner> planner,
                                 @Nonnull Class<? extends JoinOrderPlanner> joinPlanner) {
        return planner.equals(JoinPathsConjunctivePlanner.class)
                && !joinPlanner.equals(ArbitraryJoinOrderPlanner.class);
    }

    public static @Nonnull List<Supplier<ConjunctivePlanner>> suppliers;

    static {
        suppliers = new ArrayList<>();
        for (Class<? extends ConjunctivePlanner> p : plannerClasses) {
            for (Class<? extends JoinOrderPlanner> op : joinOrderPlannerClasses) {
                Supplier<ConjunctivePlanner> supplier = () -> Guice.createInjector(new AbstractModule() {
                            @Override
                            protected void configure() {
                                bind(ConjunctivePlanner.class).to(p);
                                bind(JoinOrderPlanner.class).to(op);
                            }
                        }).getInstance(ConjunctivePlanner.class);
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

    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull CQuery query) {
        assertPlanAnswers(root, query, false, false);
    }
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull Op query) {
        assertPlanAnswers(root, query, false, false);
    }
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull CQuery query,
                                         boolean allowEmptyNode, boolean forgiveFilters) {
        assertPlanAnswers(root, new QueryOp(query), allowEmptyNode, forgiveFilters);
    }
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull Op query,
                                         boolean allowEmptyNode, boolean forgiveFilters) {
        IndexedSet<Triple> triples = IndexedSet.from(query.getMatchedTriples());

        // the plan is acyclic
        assertTrue(isAcyclic(root));

        if (!allowEmptyNode) {
            assertEquals(root.modifiers().optional(), query.modifiers().optional());
            if (query.modifiers().optional() == null)
                assertFalse(root instanceof EmptyOp, "EmptyOp is not an answer!");
            // tolerate EmptyOp x if x is a child of a union that has a non-EmptyOp child
            Set<RefEquals<Op>> tolerate = new HashSet<>();
            streamPreOrder(root).filter(UnionOp.class::isInstance)
                    .forEach(o -> {
                        long c = o.getChildren().stream().filter(EmptyOp.class::isInstance).count();
                        if (c < o.getChildren().size()) {
                            o.getChildren().stream().filter(EmptyOp.class::isInstance)
                                           .map(RefEquals::of).forEach(tolerate::add);
                        }
                    });
            // tolerate EmptyOp x if x is marked optional
            streamPreOrder(root)
                    .filter(o -> o instanceof EmptyOp && o.modifiers().optional() != null)
                    .forEach(o -> tolerate.add(RefEquals.of(o)));
            assertEquals(streamPreOrder(root).filter(EmptyOp.class::isInstance)
                            .filter(o -> !tolerate.contains(RefEquals.of(o))).count(),
                         0, "There are non-tolerable EmptyOp in the plan as leaves");
        }

        // any query node should only match triples in the query
        List<Op> bad = streamPreOrder(root)
                .filter(n -> n instanceof EndpointQueryOp
                        && !triples.containsAll(n.getMatchedTriples()))
                .collect(toList());
        assertEquals(bad, emptyList());

        // any  node should only match triples in the query
        bad = streamPreOrder(root)
                .filter(n -> !triples.containsAll(n.getMatchedTriples()))
                .collect(toList());
        assertEquals(bad, emptyList());

        // the set of matched triples in the plan must be the same as the query
        assertEquals(root.getMatchedTriples(), triples);

        long queryUnions = streamPreOrder(query).filter(UnionOp.class::isInstance).count();
        if (queryUnions == 0) {
            // all nodes in a MQNode must match the exact same triples in query
            // this allows us to consider the MQNode as a unit in the plan
            bad = streamPreOrder(root).filter(n -> n instanceof UnionOp)
                    .map(n -> (UnionOp) n)
                    .filter(n -> n.getChildren().stream().map(Op::getMatchedTriples)
                            .distinct().count() != 1)
                    .collect(toList());
            assertEquals(bad, emptyList());
        }

        // children of MQ nodes may match the same triples with different triples
        // However, if two children have the same query, then their endpoints must not be
        // equivalent as this would be wasteful. Comparison must use the CQuery instead of
        // Set<Triple> since it may make sense to send the same triples with distinct
        // QueryRelevantTermAnnotations (e.g., WebAPICQEndpoint and JDBCCQEndpoint)
        List<Set<EndpointQueryOp>> equivSets = multiQueryNodes(root).stream()
                .map(n -> {
                    Set<EndpointQueryOp> equiv = new HashSet<>();
                    ListMultimap<CQuery, EndpointQueryOp> mm;
                    mm = MultimapBuilder.hashKeys().arrayListValues().build();
                    for (Op child : n.getChildren()) {
                        if (child instanceof EndpointQueryOp)
                            mm.put(((EndpointQueryOp) child).getQuery(), (EndpointQueryOp) child);
                    }
                    for (CQuery key : mm.keySet()) {
                        for (int i = 0; i < mm.get(key).size(); i++) {
                            EndpointQueryOp outer = mm.get(key).get(i);
                            for (int j = i + 1; j < mm.get(key).size(); j++) {
                                EndpointQueryOp inner = mm.get(key).get(j);
                                if (outer.getEndpoint().isAlternative(inner.getEndpoint()) ||
                                        inner.getEndpoint().isAlternative(outer.getEndpoint())) {
                                    equiv.add(outer);
                                    equiv.add(inner);
                                }
                            }
                        }
                    }
                    return equiv;
                }).filter(s -> !s.isEmpty()).collect(toList());
        assertEquals(equivSets, emptySet());

        // no single-child union  (be it a legit union or a multi-query)
        bad = streamPreOrder(root)
                .filter(n -> n instanceof UnionOp && n.getChildren().size() < 2)
                .collect(toList());
        assertEquals(bad, emptyList());

        // MQ nodes should not be directly nested (that is not elegant)
        bad = multiQueryNodes(root).stream()
                .filter(n -> n.getChildren().stream().anyMatch(n2 -> n2 instanceof UnionOp))
                .collect(toList());
        assertEquals(bad, emptyList());

        // no ConjunctionOp in the plan (should've been replaced with JoinOps)
        bad = streamPreOrder(root).filter(n -> n instanceof ConjunctionOp).collect(toList());
        assertEquals(bad, emptyList());

        // all join nodes are valid joins
        bad = streamPreOrder(root).filter(n -> n instanceof JoinOp).map(n -> (JoinOp) n)
                .filter(n -> !JoinInfo.getJoinability(n.getLeft(), n.getRight()).isValid())
                .collect(toList());
        assertEquals(bad, emptyList());

        // no single-child cartesian nodes
        bad = streamPreOrder(root)
                .filter(n -> n instanceof CartesianOp && n.getChildren().size() < 2)
                .collect(toList());
        assertEquals(bad, emptyList());

        // cartesian nodes should not be directly nested (that is not elegant)
        bad = streamPreOrder(root)
                .filter(n -> n instanceof CartesianOp
                        && n.getChildren().stream().anyMatch(n2 -> n2 instanceof CartesianOp))
                .collect(toList());
        assertEquals(bad, emptyList());

        // no cartesian nodes where a join is applicable between two of its operands
        bad = streamPreOrder(root).filter(n -> n instanceof CartesianOp)
                .filter(n -> {
                    HashSet<Op> children = new HashSet<>(n.getChildren());
                    //noinspection UnstableApiUsage
                    for (Set<Op> pair : Sets.combinations(children, 2)) {
                        Iterator<Op> it = pair.iterator();
                        Op l = it.next();
                        assert it.hasNext();
                        Op r = it.next();
                        if (JoinInfo.getJoinability(l, r).isValid())
                            return true; // found a violation
                    }
                    return false;
                }).collect(toList());

        if (!forgiveFilters) {
            Set<String> allVars = TreeUtils.streamPreOrder(query)
                    .filter(QueryOp.class::isInstance)
                    .flatMap(o -> ((QueryOp)o).getQuery().attr().tripleVarNames().stream())
                    .collect(toSet());
            List<SPARQLFilter> allFilters = streamPreOrder(query)
                    .flatMap(o -> o.modifiers().filters().stream())
                    .map(f -> {
                        if (allVars.containsAll(f.getVarTermNames()))
                            return f;
                        HashSet<String> missing = new HashSet<>(f.getVarTermNames());
                        missing.removeAll(allVars);
                        return f.withVarTermsUnbound(missing);
                    }).collect(toList());
            //all filters are placed somewhere
            List<SPARQLFilter> missingFilters = allFilters.stream()
                    .filter(f -> streamPreOrder(root).noneMatch(n -> {
                        if (n instanceof QueryOp) {
                            if (((QueryOp)n).getQuery().getModifiers().contains(f))
                                return true;
                        }
                        return n.modifiers().contains(f);
                    }))
                    .collect(toList());
            assertEquals(missingFilters, emptyList());

            // forgive unassignable filters

            // all filters are placed somewhere valid (with all required vars)
            List<ImmutablePair<? extends Op, SPARQLFilter>> badFilterAssignments;
            badFilterAssignments = streamPreOrder(root)
                    .flatMap(n -> n.modifiers().filters().stream().map(f -> ImmutablePair.of(n, f)))
                    .filter(p -> !p.left.getAllVars().containsAll(p.right.getVarTermNames()))
                    .collect(toList());
            assertEquals(badFilterAssignments, emptyList());

            // same as previous, but checks filters within CQuery instances
            badFilterAssignments = streamPreOrder(root)
                    .filter(QueryOp.class::isInstance)
                    .map(n -> (QueryOp)n)
                    .flatMap(n -> n.getQuery().getModifiers().stream()
                                              .filter(SPARQLFilter.class::isInstance)
                                              .map(f -> ImmutablePair.of(n, (SPARQLFilter)f)))
                    .filter(p -> !p.left.getAllVars().containsAll(p.right.getVarTermNames()))
                    .collect(toList());
            assertEquals(badFilterAssignments, emptyList());
        }

        assertEquals(bad, emptyList());
    }

    private static @Nonnull List<UnionOp> multiQueryNodes(@Nonnull Op root) {
        List<UnionOp> list = new ArrayList<>();
        ArrayDeque<Op> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Op op = queue.remove();
            if (op instanceof UnionOp) {
                boolean isMQ = op.getChildren().stream().map(Op::getMatchedTriples)
                                               .distinct().count() == 1;
                if (isMQ)
                    list.add((UnionOp)op);
            }
            queue.addAll(op.getChildren());
        }
        return list;
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
        Op node = planner.plan(query, singleton(queryOp));
        assertSame(node, queryOp);
        assertPlanAnswers(node, query);
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testDuplicateQuery(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = createQuery(Alice, knows, x);
        EndpointQueryOp node1 = new EndpointQueryOp(empty1, query);
        EndpointQueryOp node2 = new EndpointQueryOp(empty2, query);
        Op root = planner.plan(query, asList(node1, node2));
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
        Op root = planner.plan(query, asList(node1, node2));
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
        Op root = planner.plan(query, asList(new EndpointQueryOp(empty1, q1),
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
        CQuery query = CQuery.from(new Triple(Alice, knows, x), new Triple(y, knows, Bob));
        CQuery q1 = CQuery.from(query.get(0));
        CQuery q2 = CQuery.from(query.get(1));
        EndpointQueryOp node1 = new EndpointQueryOp(empty1, q1), node2 = new EndpointQueryOp(empty1, q2);
        Op root = planner.plan(query, asList(node1, node2));
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
            Op root = planner.plan(query, shuffled);

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
        Op root = planner.plan(query, asList(q1, q2, q3));

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
    public void testSameQuerySameEp(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(x, knows, y), new Triple(y, knows, z));
        EndpointQueryOp q1 = new EndpointQueryOp(empty1, CQuery.from(query.get(0)));
        EndpointQueryOp q2 = new EndpointQueryOp(empty1, CQuery.from(query.get(0)));
        EndpointQueryOp q3 = new EndpointQueryOp(empty1, CQuery.from(query.get(1)));

        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(asList((Op)q1, q2, q3))) {
            Op plan = planner.plan(query, permutation);
            Set<Op> qns = streamPreOrder(plan)
                    .filter(EndpointQueryOp.class::isInstance).collect(toSet());
            assertTrue(qns.contains(q3));
            assertEquals(qns.size(), 2);
            assertPlanAnswers(plan, query);
        }
    }

    @Test(dataProvider = "suppliersData", groups={"fast"})
    public void testSameQueryEquivalentEp(@Nonnull Supplier<ConjunctivePlanner> supplier) {
        ConjunctivePlanner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(x, knows, y), new Triple(y, knows, z));
        EndpointQueryOp q1 = new EndpointQueryOp(empty3a, CQuery.from(query.get(0)));
        EndpointQueryOp q2 = new EndpointQueryOp(empty3b, CQuery.from(query.get(0)));
        EndpointQueryOp q3 = new EndpointQueryOp(empty1 , CQuery.from(query.get(1)));

        //noinspection UnstableApiUsage
        for (List<Op> permutation : permutations(asList((Op)q1, q2, q3))) {
            Op plan = planner.plan(query, permutation);
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
            Op plan = planner.plan(query, nodes);
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
            Op root = planner.plan(query, permutation);
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
            Op plan = planner.plan(f.query, permutation);
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
            Op plan = planner.plan(f.query, permutation);
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
            Op plan = planner.plan(f.query, permutation);
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
            Op plan = planner.plan(f.query, permutation);
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

        Op plan = planner.plan(query, nodes);
        assertPlanAnswers(plan, query);
    }

    @DataProvider
    public static Object[][] optionalQueriesData() {
        List<Object[]> list = new ArrayList<>();
        for (Object[] row : suppliersData()) {
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, SPARQLFilter.build("?v <= 23"),
                    y, id, z
            )});
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, SPARQLFilter.build("?v <= 23"),
                    y, id, z,
                    y, dataAbertura, w //filter optionals, atom is used as output
            )});
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, SPARQLFilter.build("?v <= 23"),
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
        Op plan = planner.plan(wholeQuery, asList(n1, n2));
        assertPlanAnswers(plan, wholeQuery, false, true);
    }
}