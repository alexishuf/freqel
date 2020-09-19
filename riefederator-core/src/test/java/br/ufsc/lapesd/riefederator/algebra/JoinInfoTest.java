package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.UndirectedIrreflexiveArrayGraph;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.JoinInfo.getJoinability;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class JoinInfoTest implements TestContext {
    private static final Atom X = new Atom("X");
    private static final Atom Y = new Atom("Y");
    private static final Atom Z = new Atom("Z");


    private static final CQuery xpy = CQuery.from(new Triple(x, p1, y));
    private static final CQuery ypz = CQuery.from(new Triple(y, p1, z));
    private static final CQuery zpw = CQuery.from(new Triple(z, p1, w));
    private static final CQuery zpx = CQuery.from(new Triple(z, p1, x));
    private static final CQuery xpyfz = createQuery(x, p1, y, SPARQLFilter.build("?y < ?z"));

    private static final CQuery xpyi = createQuery(x, AtomAnnotation.of(X),
            p1, y, AtomInputAnnotation.asRequired(Y, "Y").get());
    private static final CQuery xpyio = createQuery(x, AtomAnnotation.of(X),
            p1, y, AtomInputAnnotation.asOptional(Y, "Y").get());
    private static final CQuery yipz = createQuery(y, AtomInputAnnotation.asRequired(Y, "Y").get(),
            p1, z, AtomAnnotation.of(Z));
    private static final CQuery yiopz = createQuery(y, AtomInputAnnotation.asOptional(Y, "Y").get(),
            p1, z, AtomAnnotation.of(Z));


    private static @Nonnull Op node(@Nonnull CQuery... queries) {
        return node(1, queries);
    }

    private static @Nonnull EndpointQueryOp node(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        int oldModifiersCount = query.getModifiers().size();
        Set<Var> termVars = query.attr().tripleVars();
        Set<Var> filterVars = query.getModifiers().stream().filter(SPARQLFilter.class::isInstance)
                .flatMap(m -> ((SPARQLFilter) m).getVars().stream())
                .filter(v -> !termVars.contains(v))
                .collect(toSet());
        if (!filterVars.isEmpty()) {
            MutableCQuery mQuery = new MutableCQuery(query);
            int idx = 1;
            for (Var var : filterVars) {
                mQuery.annotate(var, AtomInputAnnotation.asRequired(new Atom("A" + idx), "a"+idx).get());
                ++idx;
            }
            query = mQuery;
            assert query.getModifiers().size() == oldModifiersCount : "Lost modifiers";
        }
        return new EndpointQueryOp(endpoint, query);
    }

    private static @Nonnull Op node(int endpoints, @Nonnull CQuery... queries) {
        Preconditions.checkArgument(queries.length > 0);
        Preconditions.checkArgument(endpoints > 0);
        List<EmptyEndpoint> endpointList = new ArrayList<>(endpoints);
        for (int i = 0; i < endpoints; i++) {
            endpointList.add(new EmptyEndpoint());
        }
        if (queries.length == 1 && endpoints == 1)
            return  node(endpointList.get(0), queries[0]);
        UnionOp.Builder builder = UnionOp.builder();
        for (int i = 0; i < endpoints; i++) {
            for (CQuery query : queries)
                builder.add(node(endpointList.get(i), query));
        }
        return builder.build();
    }


    @DataProvider
    public static Object[][] plainData() {
        List<List<Object>> data = asList(
                asList(node(xpy), node(ypz), singleton("y"), emptySet(), false),
                asList(node(xpyfz), node(ypz), Sets.newHashSet("y", "z"), emptySet(), false),
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

                asList(JoinOp.create(node(xpy), node(ypz)), node(zpw),
                        singleton("z"), emptySet(), false),
                asList(JoinOp.create(node(xpy), node(ypz)), node(xpy),
                        emptySet(), emptySet(), true),
                asList(JoinOp.create(node(xpy), node(ypz)), node(ypz),
                        emptySet(), emptySet(), true),
                asList(JoinOp.create(node(xpy), node(zpx)), node(ypz),
                        asList("y", "z"), emptySet(), false),

                //ignore MultiQueryNodes at leafs
                asList(JoinOp.create(node(3, xpy), node(3, ypz)), node(3, zpw),
                        singleton("z"), emptySet(), false),
                asList(JoinOp.create(node(3, xpy), node(3, ypz)), node(3, xpy),
                        emptySet(), emptySet(), true),

                // both joins are invalid as plain joins because the union takes y as an input var
                // such joins should never occur in actual plans
                asList(node(ypz, yipz), node(xpy, xpyi), emptySet(), singleton("y"), false),
                asList(node(ypz, yipz), node(xpyi), emptySet(), singleton("y"), false),

                // the following two joins are valid because y is an optional input
                asList(node(ypz, yiopz), node(xpy, xpyio), singleton("y"), emptySet(), false),
                asList(node(ypz, yiopz), node(xpyio), singleton("y"), emptySet(), false)
        );
        return data.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "plainData")
    public void testPlain(@Nonnull Op l, @Nonnull Op r,
                          @Nonnull Collection<String> joinVars,
                          @Nonnull Collection<String> pendingInputs, boolean subsumed) {
        for (JoinInfo j : asList(JoinInfo.getJoinability(l, r), JoinInfo.getJoinability(r, l))) {
            assertEquals(j.isValid(), !joinVars.isEmpty());
            assertEquals(j.getJoinVars(), new HashSet<>(joinVars));
            assertEquals(j.getPendingRequiredInputs(), new HashSet<>(pendingInputs));
            assertEquals(j.isSubsumed(), subsumed);

            if (j.getLeftNodes().equals(singletonList(l))) {
                assertEquals(j.getRightNodes(), singletonList(r));
            } else {
                assertEquals(j.getLeftNodes(), singletonList(r));
                assertEquals(j.getRightNodes(), singletonList(l));
            }
        }
    }

    @Test
    public void testPlainSubJoin() {
        Op n1 = node(createQuery(x, p1, y, y, p1, z));
        Op n2 = node(createQuery(x, p2, z));
        JoinInfo info = getJoinability(n1, n2);
        assertTrue(info.isValid());
        assertEquals(info.getJoinVars(), Sets.newHashSet("x", "z"));
        assertEquals(info.getPendingRequiredInputs(), emptySet());
        assertEquals(info.getPendingOptionalInputs(), emptySet());
    }

    @Test
    public void testPlainSubJoinWithInput() {
        Op n1 = node(createQuery(x, p2, y, y, p2, z));
        Op n2 = node(xpyi);
        JoinInfo info = JoinInfo.getJoinability(n1, n2);
        assertTrue(info.isValid());
        assertEquals(info.getJoinVars(), Sets.newHashSet("x", "y"));
        assertEquals(info.getPendingRequiredInputs(), emptySet());
        assertEquals(info.getPendingOptionalInputs(), emptySet());
    }

    @Test(dataProvider = "plainData") @SuppressWarnings("unused")
    public void testPlainReflexive(@Nonnull Op l, @Nonnull Op r,
                                   @Nonnull Collection<String> joinVars,
                                   @Nonnull Collection<String> pendingInputs, boolean subsumed) {
        JoinInfo from = JoinInfo.getJoinability(l, r);
        JoinInfo to = JoinInfo.getJoinability(r, l);
        assertEquals(from, from);
        assertEquals(to, to);
        assertEquals(from.isValid(), to.isValid());
        assertEquals(from.getJoinVars(), to.getJoinVars());
        assertEquals(from.getPendingRequiredInputs(), to.getPendingRequiredInputs());
        assertEquals(from.isSubsumed(), to.isSubsumed());
        assertEquals(from.getLeftNodes(), to.getRightNodes());
        assertEquals(from.getRightNodes(), to.getLeftNodes());
        assertEquals(from, to);
    }

    @Test
    public void testJoinInfoGraph() {
        List<Op> nodes =
                //     0          1          2          3          4           5
                asList(node(xpy), node(ypz), node(zpw), node(zpx), node(xpyi), node(yipz));
        UndirectedIrreflexiveArrayGraph<Op, JoinInfo> g;
        g = new UndirectedIrreflexiveArrayGraph<Op, JoinInfo>(JoinInfo.class, nodes) {
            @Override
            protected @Nullable JoinInfo weigh(@Nonnull Op l, @Nonnull Op r) {
                JoinInfo info = JoinInfo.getJoinability(l, r);
                return info.isValid() ? info : null;
            }
        };
        int[][] exValid = {{0,1}, {0,3}, {0,5}, {1, 2}, {1,3}, {1,4}, {2, 3}, {2,5}, {3,4}, {3,5}};
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i+1; j < nodes.size(); j++) {
                boolean expected = false;
                for (int[] pair : exValid)
                    expected |= pair[0] == i && pair[1] == j;
                assertEquals(g.getWeight(i, j) != null, expected);
            }
        }
        assertEquals(g.getWeight(0, 5).getJoinVars(), singleton("y"));
        assertEquals(g.getWeight(0, 5).getPendingRequiredInputs(), emptySet());
        assertEquals(g.getWeight(3, 4).getJoinVars(), singleton("x"));
        assertEquals(g.getWeight(3, 4).getPendingRequiredInputs(), singleton("y"));

        List<ImmutablePair<JoinInfo, Op>> actual = new ArrayList<>();
        g.forEachNeighbor(0, (w, n) -> actual.add(ImmutablePair.of(w, n)));
        assertEquals(actual, asList(
                ImmutablePair.of(g.getWeight(0, 1), nodes.get(1)),
                ImmutablePair.of(g.getWeight(0, 3), nodes.get(3)),
                ImmutablePair.of(g.getWeight(0, 5), nodes.get(5))
        ));

        actual.clear();
        g.forEachNeighbor(nodes.get(3), (w, n) -> actual.add(ImmutablePair.of(w, n)));
        assertEquals(actual, asList(
                ImmutablePair.of(g.getWeight(0, 3), nodes.get(0)),
                ImmutablePair.of(g.getWeight(1, 3), nodes.get(1)),
                ImmutablePair.of(g.getWeight(2, 3), nodes.get(2)),
                ImmutablePair.of(g.getWeight(3, 4), nodes.get(4)),
                ImmutablePair.of(g.getWeight(3, 5), nodes.get(5))
        ));
    }

    @DataProvider
    public static Object[][] isLinkedToData() {
        Op nxpy = node(xpy), nypz = node(ypz), nyipz = node(yipz), nzpw = node(zpw);
        Op mypz = UnionOp.builder().add(nyipz).add(nypz).build();
        return Stream.of(
                asList(JoinInfo.getJoinability(nxpy, nypz), JoinInfo.getJoinability(nypz,  nzpw), true),
                asList(JoinInfo.getJoinability(nypz, nxpy), JoinInfo.getJoinability(nzpw,  nypz), true),
                asList(JoinInfo.getJoinability(nxpy, nypz), JoinInfo.getJoinability(nyipz, nzpw), false),

                asList(JoinInfo.getJoinability(nxpy, mypz), JoinInfo.getJoinability(mypz, nzpw), true),
                asList(JoinInfo.getJoinability(nxpy, mypz), JoinInfo.getJoinability(nypz, nzpw), false),
                asList(JoinInfo.getJoinability(mypz, nxpy), JoinInfo.getJoinability(nzpw, mypz), true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "isLinkedToData")
    public void testIsLinkedTo(JoinInfo info, JoinInfo other, boolean expected) {
        assertEquals(info.isLinkedTo(other), expected);
        assertEquals(other.isLinkedTo(info), expected);
    }

    @Test
    public void testGetByPosition() {
        Op n1 = node(xpy), n2 = node(ypz);
        JoinInfo info1 = JoinInfo.getJoinability(n1, n2);
        assertSame(info1.get(JoinInfo.Position.LEFT), n1);
        assertSame(info1.get(JoinInfo.Position.RIGHT), n2);
        assertEquals(info1.getNodes(JoinInfo.Position.LEFT), singleton(n1));
        assertEquals(info1.getNodes(JoinInfo.Position.RIGHT), singleton(n2));
    }


    @DataProvider
    public static Object[][] oppositeToLinkedData() {
        Op nxpy = node(xpy), nypz = node(ypz), nyipz = node(yipz), nzpw = node(zpw);
        Op mypz = UnionOp.builder().add(nyipz).add(nypz).build();
        return Stream.of(
                asList(JoinInfo.getJoinability(nxpy, nypz), JoinInfo.getJoinability(nypz, nzpw), nxpy),
                asList(JoinInfo.getJoinability(nypz, nzpw), JoinInfo.getJoinability(nxpy, nypz), nzpw),
                asList(JoinInfo.getJoinability(nypz, nxpy), JoinInfo.getJoinability(nzpw, nypz), nxpy),
                asList(JoinInfo.getJoinability(nzpw, nypz), JoinInfo.getJoinability(nypz, nxpy), nzpw),

                asList(JoinInfo.getJoinability(nypz, nxpy), JoinInfo.getJoinability(nypz, nzpw), nxpy),
                asList(JoinInfo.getJoinability(nypz, nzpw), JoinInfo.getJoinability(nypz, nxpy), nzpw),

                asList(JoinInfo.getJoinability(nxpy, nypz), JoinInfo.getJoinability(nzpw, nypz), nxpy),
                asList(JoinInfo.getJoinability(nzpw, nypz), JoinInfo.getJoinability(nxpy, nypz), nzpw),

                asList(JoinInfo.getJoinability(nxpy, mypz), JoinInfo.getJoinability(mypz, nzpw), nxpy),
                asList(JoinInfo.getJoinability(mypz, nzpw), JoinInfo.getJoinability(nxpy, mypz), nzpw),
                asList(JoinInfo.getJoinability(mypz, nxpy), JoinInfo.getJoinability(nzpw, mypz), nxpy),
                asList(JoinInfo.getJoinability(nzpw, mypz), JoinInfo.getJoinability(mypz, nxpy), nzpw)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "oppositeToLinkedData")
    public void testOppositeToLinked(JoinInfo info, JoinInfo other, Op expected) {
        assertTrue(info.isLinkedTo(other));
        assertTrue(other.isLinkedTo(info));
        assertSame(info.getOppositeToLinked(other), expected);
    }
}