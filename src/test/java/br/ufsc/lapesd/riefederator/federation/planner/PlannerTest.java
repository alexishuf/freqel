package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.federation.planner.impl.HeuristicPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.streamDepthLeft;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class PlannerTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI manages = new StdURI("http://example.org/manages");
    public static final @Nonnull StdURI title = new StdURI("http://example.org/title");
    public static final @Nonnull StdURI genre = new StdURI("http://example.org/genre");
    public static final @Nonnull StdURI genreName = new StdURI("http://example.org/genreName");
    public static final @Nonnull StdLit title1 = StdLit.fromEscaped("title 1", "en");
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar Z = new StdVar("z");
    public static final @Nonnull StdVar U = new StdVar("u");
    public static final @Nonnull StdVar V = new StdVar("v");

    public static @Nonnull List<NamedSupplier<Planner>> suppliers = singletonList(
            new NamedSupplier<>(HeuristicPlanner.class));

    private static final @Nonnull EmptyEndpoint empty1 = new EmptyEndpoint(),
                                                empty2 = new EmptyEndpoint();

    @DataProvider
    public static Object[][] suppliersData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "suppliersData")
    public void testEmpty(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        expectThrows(IllegalArgumentException.class, () -> planner.plan(Collections.emptyList()));
    }

    @Test(dataProvider = "suppliersData")
    public void testSingleQuery(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(ALICE, knows, X));
        QueryNode queryNode = new QueryNode(empty1, query);
        PlanNode node = planner.plan(singleton(queryNode));
        assertSame(node, queryNode);
    }

    @Test(dataProvider = "suppliersData")
    public void testDuplicateQuery(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery query = CQuery.from(new Triple(ALICE, knows, X));
        QueryNode node1 = new QueryNode(empty1, query);
        QueryNode node2 = new QueryNode(empty2, query);
        PlanNode root = planner.plan(asList(node1, node2));
        assertEquals(root.getResultVars(), singleton("x"));
        assertFalse(root.isProjecting());

        // no good reason for more than 3 nodes
        assertTrue(streamDepthLeft(root).count() <= 3);
    }

    @Test(dataProvider = "suppliersData")
    public void testSingleJoin(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery q1 = CQuery.from(new Triple(ALICE, knows, X));
        CQuery q2 = CQuery.from(new Triple(X, knows, Y));
        QueryNode node1 = new QueryNode(empty1, q1);
        QueryNode node2 = new QueryNode(empty1, q2);
        PlanNode root = planner.plan(asList(node1, node2));
        assertEquals(root.getResultVars(), asList("x", "y"));

        // a reasonable plan would just add a join node over the query nodes
        assertTrue(streamDepthLeft(root).count() <= 5);
        List<JoinNode> joins = streamDepthLeft(root).filter(n -> n instanceof JoinNode)
                                         .map(n -> (JoinNode)n).collect(toList());
        assertEquals(joins.size(), 1);
        assertEquals(joins.get(0).getJoinVars(), singleton("x"));
    }

    @Test(dataProvider = "suppliersData")
    public void testDoNotJoinSameVarsDifferentQueries(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery q1 = CQuery.from(asList(new Triple(X, knows, ALICE), new Triple(X, knows, Y)));
        CQuery q2 = CQuery.from(new Triple(X, manages, Y));
        PlanNode root = planner.plan(asList(new QueryNode(empty1, q1), new QueryNode(empty2, q2)));

        assertEquals(streamDepthLeft(root).filter(n -> n instanceof JoinNode).count(), 1);
        assertEquals(streamDepthLeft(root)
                              .filter(n -> n instanceof MultiQueryNode).count(), 0);
        // a sane count is 3: MultiQuery(q1, q2)
        assertTrue(streamDepthLeft(root).count() <= 4);
    }

    @Test(dataProvider = "suppliersData")
    public void testCartesianProduct(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery q1 = CQuery.from(new Triple(ALICE, knows, X));
        CQuery q2 = CQuery.from(new Triple(Y, knows, BOB));
        QueryNode node1 = new QueryNode(empty1, q1), node2 = new QueryNode(empty1, q2);
        PlanNode root = planner.plan(asList(node1, node2));
        assertEquals(root.getResultVars(), Sets.newHashSet("x", "y"));

        assertTrue(streamDepthLeft(root).count() <= 5);
        List<CartesianNode> nodes = streamDepthLeft(root)
                                             .filter(n -> n instanceof CartesianNode)
                                             .map(n -> (CartesianNode) n).collect(toList());
        assertEquals(nodes.size(), 1);
        assertEquals(nodes.get(0).getResultVars(), Sets.newHashSet("x", "y"));
        assertEquals(nodes.get(0).getChildren().size(), 2);
    }

    @Test(dataProvider = "suppliersData")
    public void testLargeTree(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        CQuery q1 = CQuery.from(new Triple(ALICE, knows, X));
        CQuery q2 = CQuery.from(new Triple(X, knows, Y));
        CQuery q3 = CQuery.from(new Triple(ALICE, knows, U));
        CQuery q4 = CQuery.from(new Triple(U, knows, V));

        List<QueryNode> leaves = asList(new QueryNode(empty1, q1), new QueryNode(empty2, q1),
                                        new QueryNode(empty1, q2),
                                        new QueryNode(empty1, q3),
                                        new QueryNode(empty1, q4), new QueryNode(empty2, q4));
        Random random = new Random(79812531);
        for (int i = 0; i < 16; i++) {
            ArrayList<QueryNode> shuffled = new ArrayList<>(leaves);
            Collections.shuffle(shuffled, random);
            PlanNode root = planner.plan(shuffled);

            Set<QueryNode> observed = streamDepthLeft(root)
                                               .filter(n -> n instanceof QueryNode)
                                               .map(n -> (QueryNode) n).collect(toSet());
            assertEquals(observed, new HashSet<>(shuffled));
            List<JoinNode> joinNodes = streamDepthLeft(root)
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

            List<CartesianNode> cartesianNodes = streamDepthLeft(root)
                                                          .filter(n -> n instanceof CartesianNode)
                                                          .map(n -> (CartesianNode) n)
                                                          .collect(toList());
            assertEquals(cartesianNodes.size(), 1);
            assertEquals(cartesianNodes.get(0).getResultVars(),
                         Sets.newHashSet("x", "y", "u", "v"));
            assertEquals(cartesianNodes.get(0).getChildren().size(), 2);
        }
    }

    @Test(dataProvider = "suppliersData")
    public void testBookShop(@Nonnull Supplier<Planner> supplier) {
        Planner planner = supplier.get();
        QueryNode q1 = new QueryNode(empty1, CQuery.from(new Triple(X, title, title1)));
        QueryNode q2 = new QueryNode(empty1, CQuery.from(new Triple(X, genre, Y)));
        QueryNode q3 = new QueryNode(empty2, CQuery.from(new Triple(Y, genreName, Z)));
        PlanNode root = planner.plan(asList(q1, q2, q3));

        assertEquals(streamDepthLeft(root).filter(n -> n instanceof CartesianNode).count(), 0);
        assertEquals(streamDepthLeft(root).filter(n -> n instanceof JoinNode).count(), 2);

        Set<CQuery> queries = streamDepthLeft(root).filter(n -> n instanceof QueryNode)
                .map(n -> ((QueryNode) n).getQuery()).collect(toSet());
        HashSet<CQuery> expectedQueries = Sets.newHashSet(
                CQuery.from(new Triple(X, title, title1)),
                CQuery.from(new Triple(X, genre, Y)),
                CQuery.from(new Triple(Y, genreName, Z))
        );
        assertEquals(queries, expectedQueries);
    }

}