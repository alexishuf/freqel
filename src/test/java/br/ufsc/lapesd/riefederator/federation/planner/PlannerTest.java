package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.impl.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.HeuristicPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinPathsPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getMultiJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getPlainJoinability;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.*;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class PlannerTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI name = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdURI manages = new StdURI("http://example.org/manages");
    public static final @Nonnull StdURI title = new StdURI("http://example.org/title");
    public static final @Nonnull StdURI genre = new StdURI("http://example.org/genre");
    public static final @Nonnull StdURI author = new StdURI("http://example.org/author");
    public static final @Nonnull StdURI genreName = new StdURI("http://example.org/genreName");
    public static final @Nonnull StdLit title1 = StdLit.fromEscaped("title 1", "en");
    public static final @Nonnull StdLit author1 = StdLit.fromUnescaped("author 1", "en");
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar Z = new StdVar("z");
    public static final @Nonnull StdVar U = new StdVar("u");
    public static final @Nonnull StdVar V = new StdVar("v");

    public static @Nonnull List<NamedSupplier<Planner>> suppliers = asList(
            new NamedSupplier<>(HeuristicPlanner.class),
            new NamedSupplier<>("JoinPathsPlanner+ArbitraryJoinOrderPlanner",
                    () -> new JoinPathsPlanner(new ArbitraryJoinOrderPlanner()))
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
        expectThrows(IllegalArgumentException.class, () -> planner.plan(CQuery.from(), emptyList()));
    }

    @Test(dataProvider = "suppliersData")
    public void testSingleQuery(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(ALICE, knows, X));
        QueryNode queryNode = new QueryNode(empty1, query);
        PlanNode node = planner.plan(query, singleton(queryNode));
        assertSame(node, queryNode);
        assertPlanAnswers(node, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testDuplicateQuery(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(ALICE, knows, X));
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
        CQuery query = CQuery.from(new Triple(ALICE, knows, X),
                                   new Triple(X, knows, Y));
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
                new Triple(X, knows, ALICE), new Triple(X, knows, Y), new Triple(X, manages, Y)
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
        CQuery query = CQuery.from(new Triple(ALICE, knows, X), new Triple(Y, knows, BOB));
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
                new Triple(ALICE, knows, X),
                new Triple(X, knows, Y),
                new Triple(ALICE, knows, U),
                new Triple(U, knows, V)
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

    protected void assertValidJoins(@Nonnull PlanNode root) {
        String msg = checkValidJoins(root, emptySet());
        if (msg != null)
            fail(msg);
    }

    protected String checkValidJoins(@Nonnull PlanNode root, @Nonnull Set<String> allowedInputs) {
        if (root instanceof JoinNode) {
            JoinNode join = (JoinNode) root;
            List<PlanNode> left = childrenIfMulti(join.getLeft());
            List<PlanNode> right = childrenIfMulti(join.getRight());
            String msg;
            msg = checkHasCounterpart(allowedInputs, join, left, right, "Left");
            if (msg != null) return msg;
            return checkHasCounterpart(allowedInputs, join, right, left, "Right");
        } else {
            for (PlanNode child : root.getChildren()) {
                String msg = checkValidJoins(child, allowedInputs);
                if (msg != null) return msg;
            }
            return null; //no error
        }
    }

    private String checkHasCounterpart(@Nonnull Set<String> allowedInputs, JoinNode join,
                                     List<PlanNode> outerSide, List<PlanNode> innerSide,
                                     String outerName) {
        outer:
        for (PlanNode l : outerSide) {
            for (PlanNode r : innerSide) {
                Set<String> pending = new HashSet<>();
                Set<String> vars = joinVars(l, r, pending);
                if (!vars.isEmpty() && allowedInputs.containsAll(pending)) {
                    HashSet<String> lAllowedInputs = new HashSet<>(allowedInputs);
                    lAllowedInputs.addAll(setMinus(r.getResultVars(), r.getInputVars()));
                    if (checkValidJoins(l, lAllowedInputs) != null)
                        continue; //try other r

                    HashSet<String> rAllowedInputs = new HashSet<>(allowedInputs);
                    rAllowedInputs.addAll(setMinus(l.getResultVars(), l.getInputVars()));
                    if (checkValidJoins(r, rAllowedInputs) != null)
                        continue; //try other r
                    continue outer; //l ⋈ r is OK!
                }
            }
            return outerName+" node "+l+" of join node "+join+" has no compatible counterpart";
        }
        return null; //no error
    }

    @Test(dataProvider = "suppliersData")
    public void testBookShop(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();

        CQuery query = CQuery.from(new Triple(X, title, title1),
                                   new Triple(X, genre, Y),
                                   new Triple(Y, genreName, Z));
        QueryNode q1 = new QueryNode(empty1, CQuery.from(query.get(0)));
        QueryNode q2 = new QueryNode(empty1, CQuery.from(query.get(1)));
        QueryNode q3 = new QueryNode(empty2, CQuery.from(query.get(2)));
        PlanNode root = planner.plan(query, asList(q1, q2, q3));

        assertEquals(streamPreOrder(root).filter(n -> n instanceof CartesianNode).count(), 0);
        assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinNode).count(), 2);

        Set<CQuery> queries = streamPreOrder(root).filter(n -> n instanceof QueryNode)
                .map(n -> ((QueryNode) n).getQuery()).collect(toSet());
        HashSet<CQuery> expectedQueries = Sets.newHashSet(
                CQuery.from(new Triple(X, title, title1)),
                CQuery.from(new Triple(X, genre, Y)),
                CQuery.from(new Triple(Y, genreName, Z))
        );
        assertEquals(queries, expectedQueries);
        assertValidJoins(root);
        assertPlanAnswers(root, query);
    }

    @Test(dataProvider = "suppliersData")
    public void testSameQuerySameEp(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(X, knows, Y), new Triple(Y, knows, Z));
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
            assertValidJoins(plan);
            assertPlanAnswers(plan, query);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testSameQueryEquivalentEp(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(X, knows, Y), new Triple(Y, knows, Z));
        QueryNode q1 = new QueryNode(empty3a, CQuery.from(query.get(0)));
        QueryNode q2 = new QueryNode(empty3b, CQuery.from(query.get(0)));
        QueryNode q3 = new QueryNode(empty1 , CQuery.from(query.get(1)));

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(asList(q1, q2, q3))) {
            PlanNode plan = planner.plan(query, permutation);
            Set<PlanNode> qns = streamPreOrder(plan).filter(QueryNode.class::isInstance).collect(toSet());
            assertTrue(qns.contains(q3));
            assertEquals(qns.size(), 2);
            assertValidJoins(plan);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void booksByAuthorWithIncompatibleService(Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        QueryNode q1 = new QueryNode(empty1, CQuery.from(new Triple(Y, name, author1)));
        QueryNode q2 = new QueryNode(empty2, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.asRequired(Book))
                .annotate(Y, AtomAnnotation.of(Person)).build());
        CQuery query = CQuery.from(new Triple(Y, name, author1), new Triple(X, author, Y));

        for (List<QueryNode> nodes : asList(asList(q1, q2), asList(q2, q1))) {
            PlanNode plan = planner.plan(query, nodes);
            assertTrue(plan instanceof EmptyNode);
            assertEquals(plan.getResultVars(), Sets.newHashSet("x", "y"));
            assertEquals(plan.getInputVars(), emptySet());
            assertValidJoins(plan);
            assertPlanAnswers(plan, query, true);
        }
    }

    public void booksByAuthorWithServiceTest(@Nonnull Planner planner, boolean markAsAlternatives,
                                             boolean addFromSubject) {
        CQEndpoint e2 = markAsAlternatives ? empty3a : empty2;
        CQEndpoint e3 = markAsAlternatives ? empty3b : empty4;

        QueryNode q1 = new QueryNode(empty1, CQuery.from(new Triple(Y, name, author1)));
        QueryNode q2 = new QueryNode(e2, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.asRequired(Book))
                .annotate(Y, AtomAnnotation.of(Person)).build());
        QueryNode q3 = new QueryNode(e3, CQuery.with(new Triple(X, author, Y))
                .annotate(X, AtomAnnotation.of(Book))
                .annotate(Y, AtomAnnotation.asRequired(Person)).build());
        List<QueryNode> nodes = addFromSubject ? asList(q1, q2, q3) : asList(q1, q3);
        CQuery query = CQuery.from(new Triple(Y, name, author1),
                                   new Triple(X, author, Y),
                                   new Triple(X, author, Y));

        //noinspection UnstableApiUsage
        for (List<QueryNode> permutation : permutations(nodes)) {
            PlanNode root = planner.plan(query, permutation);
            Set<PlanNode> leaves = streamPreOrder(root)
                    .filter(n -> n instanceof QueryNode).collect(toSet());
            assertEquals(leaves, Sets.newHashSet(q1, q3));
            assertEquals(streamPreOrder(root).filter(n -> n instanceof JoinNode).count(), 1);
            assertTrue(streamPreOrder(root).count() <= 4);
            assertValidJoins(root);
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
            query = CQuery.from(new Triple(ALICE, knows, X),
                                new Triple(X, knows, Y),
                                new Triple(Y, knows, BOB));
            q1 = new QueryNode(epFromAlice, CQuery.with(new Triple(ALICE, knows, X))
                    .annotate(ALICE, AtomAnnotation.asRequired(Person))
                    .annotate(X, AtomAnnotation.of(KnownPerson)).build());
            q2 = new QueryNode(epFromAlice, CQuery.with(new Triple(X, knows, Y))
                    .annotate(X, AtomAnnotation.asRequired(Person))
                    .annotate(Y, AtomAnnotation.of(KnownPerson)).build());
            q3 = new QueryNode(epFromAlice, CQuery.with(new Triple(Y, knows, BOB))
                    .annotate(Y, AtomAnnotation.asRequired(Person))
                    .annotate(BOB, AtomAnnotation.of(KnownPerson)).build());

            p1 = new QueryNode(epFromBob, CQuery.with(new Triple(ALICE, knows, X))
                    .annotate(ALICE, AtomAnnotation.of(Person))
                    .annotate(X, AtomAnnotation.asRequired(KnownPerson)).build());
            p2 = new QueryNode(epFromBob, CQuery.with(new Triple(X, knows, Y))
                    .annotate(X, AtomAnnotation.of(Person))
                    .annotate(Y, AtomAnnotation.asRequired(KnownPerson)).build());
            p3 = new QueryNode(epFromBob, CQuery.with(new Triple(Y, knows, BOB))
                    .annotate(Y, AtomAnnotation.of(Person))
                    .annotate(BOB, AtomAnnotation.asRequired(KnownPerson)).build());
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
            assertValidJoins(plan);
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
            assertValidJoins(plan);
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
            assertValidJoins(plan);
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
            assertEquals(plan.getInputVars(), emptySet());
            assertEquals(plan.getResultVars(), Sets.newHashSet("x", "y"));
            assertValidJoins(plan);
            assertPlanAnswers(plan, f.query, true);
        }
    }
}