package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.federation.planner.impl.Joinability.getMultiJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.impl.Joinability.getPlainJoinability;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JoinabilityTest {
    private static final URI p = new StdURI("http://example.org/p1");
    private static final Var x = new StdVar("x");
    private static final Var y = new StdVar("y");
    private static final Var z = new StdVar("z");
    private static final Var w = new StdVar("w");
    private static final Var u = new StdVar("u");
    private static final Var v = new StdVar("v");

    private static final Atom X = new Atom("X");
    private static final Atom Y = new Atom("Y");
    private static final Atom Z = new Atom("Z");
    private static final Atom W = new Atom("W");


    private static final CQuery xpy = CQuery.from(new Triple(x, p, y));
    private static final CQuery ypz = CQuery.from(new Triple(y, p, z));
    private static final CQuery zpw = CQuery.from(new Triple(z, p, w));
    private static final CQuery zpx = CQuery.from(new Triple(z, p, x));

    private static final CQuery xpyi = CQuery.with(new Triple(x, p, y))
            .annotate(x, AtomAnnotation.of(X))
            .annotate(y, AtomAnnotation.asRequired(Y)).build();
    private static final CQuery yipz = CQuery.with(new Triple(y, p, z))
            .annotate(y, AtomAnnotation.asRequired(Y))
            .annotate(z, AtomAnnotation.of(Z)).build();


    private static @Nonnull PlanNode node(@Nonnull CQuery... queries) {
        return node(1, queries);
    }

    private static @Nonnull PlanNode node(int endpoints, @Nonnull CQuery... queries) {
        Preconditions.checkArgument(queries.length > 0);
        Preconditions.checkArgument(endpoints > 0);
        List<EmptyEndpoint> endpointList = new ArrayList<>(endpoints);
        for (int i = 0; i < endpoints; i++) {
            endpointList.add(new EmptyEndpoint());
        }
        if (queries.length == 1 && endpoints == 1)
            return new QueryNode(endpointList.get(0), queries[0]);
        MultiQueryNode.Builder builder = MultiQueryNode.builder();
        for (int i = 0; i < endpoints; i++) {
            for (CQuery query : queries)
                builder.add(new QueryNode(endpointList.get(i), query));
        }
        return builder.build();
    }


    @DataProvider
    public static Object[][] plainData() {
        List<List<Object>> data = asList(
                asList(node(xpy), node(ypz), singleton("y"), emptySet(), false),
                asList(node(xpy), node(zpw), emptySet(), emptySet(), false),
                asList(node(xpy), node(xpy), emptySet(), emptySet(), true),
                asList(node(xpyi), node(ypz), singleton("y"), emptySet(), false),
                asList(node(xpyi), node(zpx), singleton("x"), singleton("y"), false),

                //ignore MultiQueryNodes
                asList(node(3, xpy), node(3, ypz), singleton("y"), emptySet(), false),
                asList(node(3, xpy), node(3, zpw), emptySet(), emptySet(), false),
                asList(node(3, xpy), node(3, xpy), emptySet(), emptySet(), true),
                asList(node(3, xpyi), node(3, ypz), singleton("y"), emptySet(), false),
                asList(node(3, xpyi), node(3, zpx), singleton("x"), singleton("y"), false),

                asList(JoinNode.builder(node(xpy), node(ypz)).build(), node(zpw),
                        singleton("z"), emptySet(), false),
                asList(JoinNode.builder(node(xpy), node(ypz)).build(), node(xpy),
                        emptySet(), emptySet(), true),
                asList(JoinNode.builder(node(xpy), node(ypz)).build(), node(ypz),
                        emptySet(), emptySet(), true),
                asList(JoinNode.builder(node(xpy), node(zpx)).build(), node(ypz),
                        asList("y", "z"), emptySet(), false),

                //ignore MultiQueryNodes at leafs
                asList(JoinNode.builder(node(3, xpy), node(3, ypz)).build(), node(3, zpw),
                        singleton("z"), emptySet(), false),
                asList(JoinNode.builder(node(3, xpy), node(3, ypz)).build(), node(3, xpy),
                        emptySet(), emptySet(), true),

                //ignore MultiQueryNodes that have bad child joins
                asList(node(ypz, yipz), node(xpy, xpyi), singleton("y"), emptySet(), false),
                asList(node(ypz, yipz), node(xpyi), singleton("y"), emptySet(), false)
        );
        return data.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "plainData")
    public void testPlain(@Nonnull PlanNode l, @Nonnull PlanNode r,
                          @Nonnull Collection<String> joinVars,
                          @Nonnull Collection<String> pendingInputs, boolean subsumed) {
        for (Joinability j : asList(getPlainJoinability(l, r), getPlainJoinability(r, l))) {
            assertEquals(j.isValid(), !joinVars.isEmpty());
            assertEquals(j.getJoinVars(), new HashSet<>(joinVars));
            assertEquals(j.getPendingInputs(), new HashSet<>(pendingInputs));
            assertEquals(j.isSubsumed(), subsumed);

            if (j.getLeftNodes().equals(singletonList(l))) {
                assertEquals(j.getRightNodes(), singletonList(r));
            } else {
                assertEquals(j.getLeftNodes(), singletonList(r));
                assertEquals(j.getRightNodes(), singletonList(l));
            }
        }
    }

    @Test(dataProvider = "plainData")
    public void testPlainReflexive(@Nonnull PlanNode l, @Nonnull PlanNode r,
                                   @Nonnull Collection<String> joinVars,
                                   @Nonnull Collection<String> pendingInputs, boolean subsumed) {
        Joinability from = getPlainJoinability(l, r);
        Joinability to = getPlainJoinability(r, l);
        assertEquals(from, from);
        assertEquals(to, to);
        assertEquals(from.isValid(), to.isValid());
        assertEquals(from.getJoinVars(), to.getJoinVars());
        assertEquals(from.getPendingInputs(), to.getPendingInputs());
        assertEquals(from.isSubsumed(), to.isSubsumed());
        assertEquals(from.getChildJoins(), to.getChildJoins());
        assertEquals(from.getLeftNodes(), to.getRightNodes());
        assertEquals(from.getRightNodes(), to.getLeftNodes());
        assertEquals(from, to);
    }

    @DataProvider
    public static Object[][] multiData() {
        PlanNode nxpy = node(xpy);
        PlanNode nypz = node(ypz);
        PlanNode nyipz = node(yipz);
        PlanNode nxpyi = node(xpyi);
        PlanNode mxpyi = MultiQueryNode.builder().add(nxpy).add(nxpyi).build();
        PlanNode myipz = MultiQueryNode.builder().add(nypz).add(nyipz).build();

        return Stream.of(
                asList(nxpy, nypz, singletonList("y"), emptySet(), false,
                        singletonList(ImmutablePair.of(nxpy, nypz))),
                asList(nxpy, node(xpy), emptySet(), emptySet(), true, emptyList()),
                asList(nxpy, node(xpyi), emptySet(), emptySet(), true, emptyList()),
                asList(nxpy, node(xpy, xpyi), emptySet(), emptySet(), true, emptyList()),
                asList(mxpyi, nypz, singletonList("y"), emptySet(), false,
                       asList(ImmutablePair.of(nxpy, nypz), ImmutablePair.of(nxpyi, nypz))),
                asList(mxpyi, nyipz, singletonList("y"), emptySet(), false,
                       singletonList(ImmutablePair.of(nxpy, nyipz))),
                asList(mxpyi, myipz, singletonList("y"), emptySet(), false,
                       asList(ImmutablePair.of(nxpy, nypz), ImmutablePair.of(nxpy, nyipz),
                              ImmutablePair.of(nxpyi, nypz)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "multiData")
    public void testMultiJoinability(@Nonnull PlanNode l, @Nonnull PlanNode r,
                                     @Nonnull Collection<String> joinVars,
                                     @Nonnull Collection<String> pendingInputs, boolean subsumed,
                                     @Nonnull List<ImmutablePair<PlanNode, PlanNode>> pairs) {
        for (Joinability j : asList(getMultiJoinability(l, r), getMultiJoinability(r, l))) {
            assertEquals(j.isValid(), !joinVars.isEmpty());
            assertEquals(j.getJoinVars(), new HashSet<>(joinVars));
            assertEquals(j.getPendingInputs(), new HashSet<>(pendingInputs));
            assertEquals(j.isSubsumed(), subsumed);

            List<PlanNode> lns = l instanceof MultiQueryNode ? l.getChildren() : singletonList(l);
            List<PlanNode> rns = r instanceof MultiQueryNode ? r.getChildren() : singletonList(r);
            if (j.getLeft() == l) {
                assertEquals(j.getLeftNodes(), lns);
                assertEquals(j.getRightNodes(), rns);
            } else {
                assertEquals(j.getLeftNodes(), rns);
                assertEquals(j.getRightNodes(), lns);

                //invert pairs, since we are on the second iteration
                List<ImmutablePair<PlanNode, PlanNode>> tmp = new ArrayList<>();
                for (ImmutablePair<PlanNode, PlanNode> pair : pairs)
                    tmp.add(ImmutablePair.of(pair.right, pair.left));
                pairs = tmp;
            }

            assertEquals(j.getChildJoins().keySet(), new HashSet<>(pairs));
            for (ImmutablePair<PlanNode, PlanNode> pair : pairs) {
                Joinability j2 = j.getChildJoins().get(pair);
                assertTrue(j2.isValid());
                assertEquals(j2, getPlainJoinability(pair.left, pair.right));
            }
        }
    }

}