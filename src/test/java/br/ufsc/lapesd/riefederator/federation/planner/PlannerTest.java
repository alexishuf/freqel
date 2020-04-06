package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.impl.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinPathsPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.*;
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
import br.ufsc.lapesd.riefederator.webapis.ProcurementServiceTestContext;
import br.ufsc.lapesd.riefederator.webapis.ProcurementsService;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
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

import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getMultiJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getPlainJoinability;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.isAcyclic;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.*;

public class PlannerTest implements ProcurementServiceTestContext {
    public static final @Nonnull StdLit title1 = StdLit.fromEscaped("title 1", "en");
    public static final @Nonnull StdLit author1 = StdLit.fromUnescaped("author 1", "en");

    private static final URI o3 = new StdURI("http://example.org/o3");
    private static final Var a = new StdVar("a");
    private static final Var b = new StdVar("b");
    private static final Var c = new StdVar("c");
    private static final Var d = new StdVar("d");
    private static final Var s = new StdVar("s");
    private static final Var t = new StdVar("t");

    public static @Nonnull List<NamedSupplier<Planner>> suppliers = asList(
            new NamedSupplier<>("JoinPathsPlanner+ArbitraryJoinOrderPlanner",
                    () -> new JoinPathsPlanner(new ArbitraryJoinOrderPlanner())),
            new NamedSupplier<>("JoinPathsPlanner+GreedyJoinOrderPlanner",
                    () -> new JoinPathsPlanner(new GreedyJoinOrderPlanner()))
    );

    private static final @Nonnull
    EmptyEndpoint empty1  = new EmptyEndpoint(),  empty2 = new EmptyEndpoint(),
                  empty3a = new EmptyEndpoint(), empty3b = new EmptyEndpoint(),
                  empty4  = new EmptyEndpoint();


    public static final @Nonnull Atom Book = new Atom("Book");
    public static final @Nonnull Atom Person = new Atom("Person");
    public static final @Nonnull Atom KnownPerson = new Atom("KnownPerson");
    public static final @Nonnull Atom LikedPerson = new Atom("LikedPerson");
    public static final @Nonnull Atom PersonName = new Atom("PersonName");

    static {
        empty3a.addAlternative(empty3b);
        empty3b.addAlternative(empty3a);
    }

    public static void assertPlanAnswers(@Nonnull PlanNode root, @Nonnull CQuery query) {
        assertPlanAnswers(root, query, false);
    }
    public static void assertPlanAnswers(@Nonnull PlanNode root, @Nonnull CQuery query,
                                         boolean allowEmptyNode) {
        IndexedSet<Triple> triples = IndexedSet.from(query.getMatchedTriples());

        // the plan is acyclic
        assertTrue(isAcyclic(root));

        if (!allowEmptyNode) {
            assertFalse(root instanceof EmptyNode, "EmptyNode is not an answer!");
            assertEquals(streamPreOrder(root).filter(EmptyNode.class::isInstance).count(),
                         0, "There are EmptyNodes in the plan as leaves");
        }

        // any query node should only match triples in the query
        List<PlanNode> bad = streamPreOrder(root)
                .filter(n -> n instanceof QueryNode
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

        // all nodes in a MQNode must match the exact same triples in query
        // this allows us to consider the MQNode as a unit in the plan
        bad = streamPreOrder(root).filter(n -> n instanceof MultiQueryNode)
                .map(n -> (MultiQueryNode) n)
                .filter(n -> n.getChildren().stream().map(PlanNode::getMatchedTriples)
                        .distinct().count() != 1)
                .collect(toList());
        assertEquals(bad, emptyList());

        // children of MQ nodes may match the same triples with different triples
        // However, if two children have the same triples as query, then their endpoints
        // must not be equivalent as this would be wasteful
        List<Set<QueryNode>> equivSets = streamPreOrder(root)
                .filter(n -> n instanceof MultiQueryNode)
                .map(n -> {
                    Set<QueryNode> equiv = new HashSet<>();
                    ListMultimap<Set<Triple>, QueryNode> mm;
                    mm = MultimapBuilder.hashKeys().arrayListValues().build();
                    for (PlanNode child : n.getChildren()) {
                        if (child instanceof QueryNode)
                            mm.put(((QueryNode) child).getQuery().getSet(), (QueryNode) child);
                    }
                    for (Set<Triple> key : mm.keySet()) {
                        for (int i = 0; i < mm.get(key).size(); i++) {
                            QueryNode outer = mm.get(key).get(i);
                            for (int j = i + 1; j < mm.get(key).size(); j++) {
                                QueryNode inner = mm.get(key).get(j);
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

        // no single-child MQ nodes
        bad = streamPreOrder(root)
                .filter(n -> n instanceof MultiQueryNode && n.getChildren().size() < 2)
                .collect(toList());
        assertEquals(bad, emptyList());

        // MQ nodes should not be directly nested (that is not elegant)
        bad = streamPreOrder(root)
                .filter(n -> n instanceof MultiQueryNode
                          && n.getChildren().stream().anyMatch(n2 -> n2 instanceof MultiQueryNode))
                .collect(toList());
        assertEquals(bad, emptyList());

        // all join nodes are valid joins
        bad = streamPreOrder(root).filter(n -> n instanceof JoinNode).map(n -> (JoinNode) n)
                .filter(n -> !getPlainJoinability(n.getLeft(), n.getRight()).isValid())
                .collect(toList());
        assertEquals(bad, emptyList());

        // all join nodes with MQ operands are valid
        bad = streamPreOrder(root).filter(n -> n instanceof JoinNode).map(n -> (JoinNode) n)
                .filter(n -> !getMultiJoinability(n.getLeft(), n.getRight()).isValid())
                .collect(toList());
        assertEquals(bad, emptyList());

        // no single-child cartesian nodes
        bad = streamPreOrder(root)
                .filter(n -> n instanceof CartesianNode && n.getChildren().size() < 2)
                .collect(toList());
        assertEquals(bad, emptyList());

        // cartesian nodes should not be directly nested (that is not elegant)
        bad = streamPreOrder(root)
                .filter(n -> n instanceof CartesianNode
                        && n.getChildren().stream().anyMatch(n2 -> n2 instanceof CartesianNode))
                .collect(toList());
        assertEquals(bad, emptyList());

        // no cartesian nodes where a join is applicable between two of its operands
        bad = streamPreOrder(root).filter(n -> n instanceof CartesianNode)
                .filter(n -> {
                    HashSet<PlanNode> children = new HashSet<>(n.getChildren());
                    //noinspection UnstableApiUsage
                    for (Set<PlanNode> pair : Sets.combinations(children, 2)) {
                        Iterator<PlanNode> it = pair.iterator();
                        PlanNode l = it.next();
                        assert it.hasNext();
                        PlanNode r = it.next();
                        if (getMultiJoinability(l, r).isValid())
                            return true; // found a violation
                    }
                    return false;
                }).collect(toList());
        assertEquals(bad, emptyList());
    }

    @DataProvider
    public static Object[][] suppliersData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "suppliersData")
    public void testEmpty(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        expectThrows(IllegalArgumentException.class, () -> planner.plan(createQuery(), emptyList()));
    }

    @Test(dataProvider = "suppliersData")
    public void testSingleQuery(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = createQuery(Alice, knows, x);
        QueryNode queryNode = new QueryNode(empty1, query);
        PlanNode node = planner.plan(query, singleton(queryNode));
        assertSame(node, queryNode);
        assertPlanAnswers(node, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testDuplicateQuery(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = createQuery(Alice, knows, x);
        QueryNode node1 = new QueryNode(empty1, query);
        QueryNode node2 = new QueryNode(empty2, query);
        PlanNode root = planner.plan(query, asList(node1, node2));
        assertEquals(root.getResultVars(), singleton("x"));
        assertFalse(root.isProjecting());

        // no good reason for more than 3 nodes
        assertTrue(streamPreOrder(root).count() <= 3);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testSingleJoin(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(Alice, knows, x),
                                   new Triple(x, knows, y));
        CQuery q1 = CQuery.from(query.get(0));
        CQuery q2 = CQuery.from(query.get(1));
        QueryNode node1 = new QueryNode(empty1, q1);
        QueryNode node2 = new QueryNode(empty1, q2);
        PlanNode root = planner.plan(query, asList(node1, node2));
        assertEquals(root.getResultVars(), asList("x", "y"));

        // a reasonable plan would just add a join node over the query nodes
        assertTrue(streamPreOrder(root).count() <= 5);
        List<JoinNode> joins = streamPreOrder(root).filter(n -> n instanceof JoinNode)
                                         .map(n -> (JoinNode)n).collect(toList());
        assertEquals(joins.size(), 1);
        assertEquals(joins.get(0).getJoinVars(), singleton("x"));

        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testDoNotJoinSameVarsDifferentQueries(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(
                new Triple(x, knows, Alice), new Triple(x, knows, y), new Triple(x, manages, y)
        );
        CQuery q1 = CQuery.from(query.get(0), query.get(1));
        CQuery q2 = CQuery.from(query.get(2));
        PlanNode root = planner.plan(query, asList(new QueryNode(empty1, q1),
                                                   new QueryNode(empty2, q2)));

        assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinNode).count(), 1);
        assertEquals(streamPreOrder(root)
                              .filter(n -> n instanceof MultiQueryNode).count(), 0);
        // a sane count is 3: MultiQuery(q1, q2)
        assertTrue(streamPreOrder(root).count() <= 4);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testCartesianProduct(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(Alice, knows, x), new Triple(y, knows, Bob));
        CQuery q1 = CQuery.from(query.get(0));
        CQuery q2 = CQuery.from(query.get(1));
        QueryNode node1 = new QueryNode(empty1, q1), node2 = new QueryNode(empty1, q2);
        PlanNode root = planner.plan(query, asList(node1, node2));
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));

        assertTrue(streamPreOrder(root).count() <= 5);
        List<CartesianNode> nodes = streamPreOrder(root)
                                             .filter(n -> n instanceof CartesianNode)
                                             .map(n -> (CartesianNode) n).collect(toList());
        assertEquals(nodes.size(), 1);
        assertEquals(nodes.get(0).getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(nodes.get(0).getChildren().size(), 2);

        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testLargeTree(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
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

        List<QueryNode> leaves = asList(new QueryNode(empty1, q1), new QueryNode(empty2, q1),
                                        new QueryNode(empty1, q2),
                                        new QueryNode(empty1, q3),
                                        new QueryNode(empty1, q4), new QueryNode(empty2, q4));
        Random random = new Random(79812531);
        for (int i = 0; i < 16; i++) {
            ArrayList<QueryNode> shuffled = new ArrayList<>(leaves);
            Collections.shuffle(shuffled, random);
            PlanNode root = planner.plan(query, shuffled);

            Set<QueryNode> observed = streamPreOrder(root)
                                               .filter(n -> n instanceof QueryNode)
                                               .map(n -> (QueryNode) n).collect(toSet());
            assertEquals(observed, new HashSet<>(shuffled));
            List<JoinNode> joinNodes = streamPreOrder(root)
                                                .filter(n -> n instanceof JoinNode)
                                                .map(n -> (JoinNode) n).collect(toList());
            assertEquals(joinNodes.size(), 2);

            Set<Set<String>> actualSets = joinNodes.stream().map(PlanNode::getResultVars)
                                                            .collect(toSet());
            Set<Set<String>> exSets = new HashSet<>();
            exSets.add(Sets.newHashSet("x", "y"));
            exSets.add(Sets.newHashSet("u", "v"));
            assertEquals(actualSets, exSets);

            actualSets = joinNodes.stream().map(JoinNode::getJoinVars).collect(toSet());
            exSets.clear();
            exSets.add(singleton("x"));
            exSets.add(singleton("u"));
            assertEquals(actualSets, exSets);

            List<CartesianNode> cartesianNodes = streamPreOrder(root)
                                                          .filter(n -> n instanceof CartesianNode)
                                                          .map(n -> (CartesianNode) n)
                                                          .collect(toList());
            assertEquals(cartesianNodes.size(), 1);
            assertEquals(cartesianNodes.get(0).getResultVars(),
                         Sets.newHashSet("x", "y", "u", "v"));
            assertEquals(cartesianNodes.get(0).getChildren().size(), 2);

            assertPlanAnswers(root, query);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testBookShop(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();

        CQuery query = CQuery.from(new Triple(x, title, title1),
                                   new Triple(x, genre, y),
                                   new Triple(y, genreName, z));
        QueryNode q1 = new QueryNode(empty1, CQuery.from(query.get(0)));
        QueryNode q2 = new QueryNode(empty1, CQuery.from(query.get(1)));
        QueryNode q3 = new QueryNode(empty2, CQuery.from(query.get(2)));
        PlanNode root = planner.plan(query, asList(q1, q2, q3));

        assertEquals(streamPreOrder(root).filter(n -> n instanceof CartesianNode).count(), 0);
        assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinNode).count(), 2);

        Set<CQuery> queries = streamPreOrder(root).filter(n -> n instanceof QueryNode)
                .map(n -> ((QueryNode) n).getQuery()).collect(toSet());
        HashSet<CQuery> expectedQueries = Sets.newHashSet(
                createQuery(x, title, title1),
                createQuery(x, genre, y),
                createQuery(y, genreName, z)
        );
        assertEquals(queries, expectedQueries);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testSameQuerySameEp(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(x, knows, y), new Triple(y, knows, z));
        QueryNode q1 = new QueryNode(empty1, CQuery.from(query.get(0)));
        QueryNode q2 = new QueryNode(empty1, CQuery.from(query.get(0)));
        QueryNode q3 = new QueryNode(empty1, CQuery.from(query.get(1)));

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(asList(q1, q2, q3))) {
            PlanNode plan = planner.plan(query, permutation);
            Set<PlanNode> qns = streamPreOrder(plan)
                    .filter(QueryNode.class::isInstance).collect(toSet());
            assertTrue(qns.contains(q3));
            assertEquals(qns.size(), 2);
            assertPlanAnswers(plan, query);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testSameQueryEquivalentEp(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(x, knows, y), new Triple(y, knows, z));
        QueryNode q1 = new QueryNode(empty3a, CQuery.from(query.get(0)));
        QueryNode q2 = new QueryNode(empty3b, CQuery.from(query.get(0)));
        QueryNode q3 = new QueryNode(empty1 , CQuery.from(query.get(1)));

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(asList(q1, q2, q3))) {
            PlanNode plan = planner.plan(query, permutation);
            Set<PlanNode> qns = streamPreOrder(plan).filter(QueryNode.class::isInstance).collect(toSet());
            assertTrue(qns.contains(q3));
            assertEquals(qns.size(), 2);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void booksByAuthorWithIncompatibleService(Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        QueryNode q1 = new QueryNode(empty1, createQuery(y, name, author1));
        QueryNode q2 = new QueryNode(empty2, CQuery.with(new Triple(x, author, y))
                .annotate(x, asRequired(Book, "Book"))
                .annotate(y, AtomAnnotation.of(Person)).build());
        CQuery query = CQuery.from(new Triple(y, name, author1), new Triple(x, author, y));

        for (List<QueryNode> nodes : asList(asList(q1, q2), asList(q2, q1))) {
            PlanNode plan = planner.plan(query, nodes);
            assertTrue(plan instanceof EmptyNode);
            assertEquals(plan.getResultVars(), Sets.newHashSet("x", "y"));
            assertEquals(plan.getRequiredInputVars(), emptySet());
            assertPlanAnswers(plan, query, true);
        }
    }

    public void booksByAuthorWithServiceTest(@Nonnull Planner planner, boolean markAsAlternatives,
                                             boolean addFromSubject) {
        CQEndpoint e2 = markAsAlternatives ? empty3a : empty2;
        CQEndpoint e3 = markAsAlternatives ? empty3b : empty4;

        QueryNode q1 = new QueryNode(empty1, createQuery(y, name, author1));
        assertEquals(q1.getStrictResultVars(), singleton("y"));
        QueryNode q2 = new QueryNode(e2, CQuery.with(new Triple(x, author, y))
                .annotate(x, asRequired(Book, "Book"))
                .annotate(y, AtomAnnotation.of(Person)).build());
        QueryNode q3 = new QueryNode(e3, CQuery.with(new Triple(x, author, y))
                .annotate(x, AtomAnnotation.of(Book))
                .annotate(y, asRequired(Person, "Person")).build());
        List<QueryNode> nodes = addFromSubject ? asList(q1, q2, q3) : asList(q1, q3);
        CQuery query = CQuery.from(new Triple(y, name, author1),
                                   new Triple(x, author, y),
                                   new Triple(x, author, y));

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(nodes)) {
            PlanNode root = planner.plan(query, permutation);
            Set<PlanNode> leaves = streamPreOrder(root)
                    .filter(n -> n instanceof QueryNode).collect(toSet());
            assertEquals(leaves, Sets.newHashSet(q1, q3));
            assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinNode).count(), 1);
            assertTrue(streamPreOrder(root).count() <= 4);
            assertPlanAnswers(root, query);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testBooksByAuthorARQWithService(@Nonnull Supplier<Planner> supplier) {
        booksByAuthorWithServiceTest(supplier.get(), false, false);
    }

    @Test(dataProvider = "suppliersData")
    public void testBooksByAuthorAlternativeService(@Nonnull Supplier<Planner> supplier) {
        booksByAuthorWithServiceTest(supplier.get(), true, true);
    }

    @Test(dataProvider = "suppliersData")
    public void testBooksByAuthorLeftoverService(@Nonnull Supplier<Planner> supplier) {
        booksByAuthorWithServiceTest(supplier.get(), false, true);
    }

    private static class TwoServicePathsNodes {
        QueryNode q1, q2, q3, p1, p2, p3;
        List<QueryNode> all, fromAlice, fromBob, fromAlice2, fromBob2;
        Set<Set<QueryNode>> allowedAnswers;
        CQuery query;

        public TwoServicePathsNodes(@Nonnull CQEndpoint epFromAlice,
                                    @Nonnull CQEndpoint epFromBob) {
            query = CQuery.from(new Triple(Alice, knows, x),
                                new Triple(x, knows, y),
                                new Triple(y, knows, Bob));
            q1 = new QueryNode(epFromAlice, CQuery.with(new Triple(Alice, knows, x))
                    .annotate(Alice, asRequired(Person, "Person"))
                    .annotate(x, AtomAnnotation.of(KnownPerson)).build());
            q2 = new QueryNode(epFromAlice, CQuery.with(new Triple(x, knows, y))
                    .annotate(x, asRequired(Person, "Person"))
                    .annotate(y, AtomAnnotation.of(KnownPerson)).build());
            q3 = new QueryNode(epFromAlice, CQuery.with(new Triple(y, knows, Bob))
                    .annotate(y, asRequired(Person, "Person"))
                    .annotate(Bob, AtomAnnotation.of(KnownPerson)).build());

            p1 = new QueryNode(epFromBob, CQuery.with(new Triple(Alice, knows, x))
                    .annotate(Alice, AtomAnnotation.of(Person))
                    .annotate(x, asRequired(KnownPerson, "KnownPerson")).build());
            p2 = new QueryNode(epFromBob, CQuery.with(new Triple(x, knows, y))
                    .annotate(x, AtomAnnotation.of(Person))
                    .annotate(y, asRequired(KnownPerson, "KnownPerson")).build());
            p3 = new QueryNode(epFromBob, CQuery.with(new Triple(y, knows, Bob))
                    .annotate(y, AtomAnnotation.of(Person))
                    .annotate(Bob, asRequired(KnownPerson, "KnownPerson")).build());
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

    private void onePathTwoDirectionsWithServicesTest(@Nonnull Planner planner,
                                                      boolean useAlternative) {
        EmptyEndpoint ep1 = useAlternative ? empty3a : empty1;
        EmptyEndpoint ep2 = useAlternative ? empty3b : empty1;
        TwoServicePathsNodes f = new TwoServicePathsNodes(ep1, ep2);

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(f.all)) {
            PlanNode plan = planner.plan(f.query, permutation);
            Set<QueryNode> qns = streamPreOrder(plan).filter(QueryNode.class::isInstance)
                    .map(n -> (QueryNode)n).collect(toSet());
            assertTrue(f.allowedAnswers.contains(qns), "qns="+qns);
            assertEquals(streamPreOrder(plan).filter(JoinNode.class::isInstance).count(), 2);
            assertPlanAnswers(plan, f.query);
        }
    }

    @Test(dataProvider = "suppliersData", invocationCount = 4)
    public void testOnePathTwoDirectionsWithServicesSameEp(@Nonnull Supplier<Planner> supplier) {
        onePathTwoDirectionsWithServicesTest(supplier.get(), false);
    }

    @Test(dataProvider = "suppliersData", invocationCount = 4)
    public void testOnePathTwoDirectionsWithServicesAlternativeEp(@Nonnull Supplier<Planner> supplier) {
        onePathTwoDirectionsWithServicesTest(supplier.get(), true);
    }

    @Test(dataProvider = "suppliersData", invocationCount = 4)
    public void testOnePathTwoDirectionsUnrelatedEndpoints(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        TwoServicePathsNodes f = new TwoServicePathsNodes(empty1, empty2);
        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(f.all)) {
            PlanNode plan = planner.plan(f.query, permutation);
            assertEquals(streamPreOrder(plan).filter(QueryNode.class::isInstance).collect(toSet()),
                         new HashSet<>(f.all));
            assertTrue(streamPreOrder(plan).filter(n -> n == f.q1).count() <= 3);
            assertTrue(streamPreOrder(plan).filter(n -> n == f.p1).count() <= 1);
            assertTrue(streamPreOrder(plan).filter(n -> n == f.p3).count() <= 3);
            assertTrue(streamPreOrder(plan).filter(n -> n == f.q3).count() <= 1);
            assertPlanAnswers(plan, f.query);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testDiscardMiddleNodeInPath(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        TwoServicePathsNodes f = new TwoServicePathsNodes(empty1, empty2);
        List<QueryNode> nodes = asList(f.q1, f.q2, f.q3, f.p2);
        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(nodes)) {
            PlanNode plan = planner.plan(f.query, permutation);
            assertEquals(streamPreOrder(plan).filter(QueryNode.class::isInstance).collect(toSet()),
                         Sets.newHashSet(f.fromAlice));
            assertEquals(streamPreOrder(plan).filter(QueryNode.class::isInstance).count(), 3);
            assertPlanAnswers(plan, f.query);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testUnsatisfiablePlan(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        TwoServicePathsNodes f = new TwoServicePathsNodes(empty1, empty1);
        List<QueryNode> nodes = asList(f.q1, f.p2, f.q3);
        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(nodes)) {
            PlanNode plan = planner.plan(f.query, permutation);
            assertTrue(plan instanceof EmptyNode);
            assertEquals(plan.getRequiredInputVars(), emptySet());
            assertEquals(plan.getResultVars(), Sets.newHashSet("x", "y"));
            assertPlanAnswers(plan, f.query, true);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testNonLinearPath(@Nonnull Supplier<Planner> supplier) {
        QueryNode orgByDesc = new QueryNode(empty1, CQuery.from(
                new Triple(x, p1, t)
        ));
        QueryNode contract = new QueryNode(empty1, CQuery.with(
                new Triple(y, p2, b),
                new Triple(y, p3, c),
                new Triple(y, p4, t)
        ).annotate(t, asRequired(new Atom("A1"), "A1")).build());
        QueryNode contractById = new QueryNode(empty1, CQuery.with(
                new Triple(b, p5, o3)
        ).annotate(b, asRequired(new Atom("A2"), "A2")).build());
        QueryNode contractorByName = new QueryNode(empty1, CQuery.with(
                new Triple(c, p6, s)
        ).annotate(c, asRequired(new Atom("A3"), "A3")).build());
        QueryNode procurementsOfContractor = new QueryNode(empty1, CQuery.with(
                new Triple(s, p7, a)
        ).annotate(s, asRequired(new Atom("A4"), "A4")).build());
        QueryNode procurementById = new QueryNode(empty1, CQuery.with(
                new Triple(a, p8, d)
        ).annotate(a, asRequired(new Atom("A5"), "A5")).build());
        QueryNode modalities = new QueryNode(empty1, CQuery.from(
                new Triple(z, p9, d)
        ));

        CQuery query = CQuery.from(
                new Triple(x, p1, t),
                new Triple(y, p2, b),
                new Triple(y, p3, c),
                new Triple(y, p4, t),
                new Triple(b,  p5, o3),
                new Triple(c,  p6, s),
                new Triple(s,  p7, a),
                new Triple(a,  p8, d),
                new Triple(z, p9, d)
        );

        Planner planner = supplier.get();
        List<QueryNode> nodes = asList(contractorByName, procurementsOfContractor,
                contractById, modalities, procurementById, orgByDesc, contract);
        assertTrue(nodes.stream().allMatch(n -> query.getSet().containsAll(n.getMatchedTriples())));

        PlanNode plan = planner.plan(query, nodes);
        assertPlanAnswers(plan, query);
    }

    @DataProvider
    public static Object[][] optionalQueriesData() {
        List<Object[]> list = new ArrayList<>();
        for (Object[] row : suppliersData()) {
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, SPARQLFilter.build("?v <= ?u"),
                    y, id, z
            )});
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, SPARQLFilter.build("?v <= ?u"),
                    y, id, z,
                    y, dataAbertura, w //filter optionals, atom is used as output
            )});
            list.add(new Object[]{row[0], createQuery(
                    y, valor, v, SPARQLFilter.build("?v <= ?u"),
                    y, id, z,
                    y, unidadeGestora, y1,
                    y1, orgaoVinculado, y2,
                    y2, codigoSIAFI, y3 // optional being used as output
            )});
        }
        return list.toArray(new Object[0][]);
    }

    @Test(dataProvider = "optionalQueriesData")
    public void testJoinArqWithOptionalInputs(@Nonnull Supplier<Planner> supplier,
                                              @Nonnull CQuery webQuery) throws IOException {
        Model model = ModelFactory.createDefaultModel();
        model.add(createResource(EX+"Dummy"), createProperty(EX+"p1"), createTypedLiteral(20000));
        ARQEndpoint arqEp = ARQEndpoint.forModel(model);

        WebTarget fakeTarget = ClientBuilder.newClient().target("https://localhost:22/");
        WebAPICQEndpoint webEp = ProcurementsService.getProcurementsOptClient(fakeTarget);

        CQuery arqQuery = createQuery(x, p1, v);
        QueryNode n1 = new QueryNode(arqEp, arqQuery);
        QueryNode n2 = new QueryNode(webEp, webQuery);

        JoinInfo info = getPlainJoinability(n1, n2);
        assertTrue(info.isValid());

        Planner planner = supplier.get();
        CQuery wholeQuery = CQuery.union(arqQuery, webQuery);
        PlanNode plan = planner.plan(wholeQuery, asList(n1, n2));
        assertPlanAnswers(plan, wholeQuery);
    }
}