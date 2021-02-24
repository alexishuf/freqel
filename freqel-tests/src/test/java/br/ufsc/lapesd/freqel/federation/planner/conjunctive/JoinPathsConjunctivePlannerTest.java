package br.ufsc.lapesd.freqel.federation.planner.conjunctive;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.util.RelativeCardinalityAdder;
import br.ufsc.lapesd.freqel.cardinality.impl.DefaultInnerCardinalityComputer;
import br.ufsc.lapesd.freqel.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.inject.dagger.modules.PlanningModule;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.paths.JoinComponent;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.freqel.util.indexed.FullIndexSet.fromDistinctCopy;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;


@SuppressWarnings("UnstableApiUsage")
public class JoinPathsConjunctivePlannerTest implements TestContext {
    private static final Atom Person = new Atom("Person"), Atom1 = new Atom("Atom1");

    private static final EmptyEndpoint e1 = new EmptyEndpoint("e1"), e1a = new EmptyEndpoint("e1a"),
                                       e2 = new EmptyEndpoint("e2"), e3  = new EmptyEndpoint("e3");

    static {
        e1.addAlternative(e1a);
    }

    private static  @Nonnull EndpointQueryOp node(CQEndpoint ep, @Nonnull Consumer<MutableCQuery> setup,
                                                  @Nonnull Term... terms) {
        MutableCQuery query = new MutableCQuery();
        for (int i = 0; i < terms.length; i += 3)
            query.add(new Triple(terms[i], terms[i+1], terms[i+2]));
        setup.accept(query);
        return new EndpointQueryOp(ep, query);
    }
    private static  @Nonnull EndpointQueryOp node(CQEndpoint ep, @Nonnull Term... terms) {
        return node(ep, b -> {}, terms);
    }
    private static @Nonnull Op m(@Nonnull EndpointQueryOp... nodes) {
        Preconditions.checkArgument(nodes.length > 1);
        Preconditions.checkArgument(Arrays.stream(nodes).allMatch(Objects::nonNull));
        return UnionOp.builder().addAll(stream(nodes).collect(toList())).build();
    }
    private static @Nonnull JoinPathsConjunctivePlanner createPathsPlanner() {
        return new JoinPathsConjunctivePlanner(new ArbitraryJoinOrderPlanner(),
                new DefaultInnerCardinalityComputer(ThresholdCardinalityComparator.DEFAULT,
                                                    RelativeCardinalityAdder.DEFAULT),
                PlanningModule.prePlanner(null, FreqelConfig.createDefault(),
                        NoOpPerformanceListener.INSTANCE));
    }

    @DataProvider
    public static Object[][] pathEqualsData() {
        EndpointQueryOp n1 = node(e1, Alice, p1, x);
        EndpointQueryOp n2 = node(e1, x, p1, y);
        EndpointQueryOp n3 = node(e2, y, p1, Bob);
        RefIndexSet<Op> all = RefIndexSet.fromRefDistinct(asList(n1, n2, n3));
        return Stream.of(
                asList(new JoinComponent(all, n1), new JoinComponent(all, n1), true),
                asList(new JoinComponent(all, n1), new JoinComponent(all, n2), false),
                asList(new JoinComponent(all, n1, n2),
                       new JoinComponent(all, n1, n2), true),
                asList(new JoinComponent(all, n2, n1),
                       new JoinComponent(all, n1, n2), true),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2, n3), true),
                asList(new JoinComponent(all, n3, n2, n1),
                       new JoinComponent(all, n1, n2, n3), true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "pathEqualsData", groups = {"fast"})
    public void testPathEquals(@Nonnull JoinComponent a, @Nonnull JoinComponent b, boolean expected) {
        if (expected) {
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
        } else {
            assertNotEquals(a, b);
        }
    }

    @Test(groups = {"fast"})
    public void testBuildPath() {
        EndpointQueryOp n1 = node(e1, Alice, p1, x);
        EndpointQueryOp n2 = node(e1, x, p2, y);
        EndpointQueryOp n3 = new EndpointQueryOp(e2, createQuery(y, AtomInputAnnotation.asRequired(Atom1, "Atom1").get(),
                        p3, Bob, AtomAnnotation.of(Person)));
        RefIndexSet<Op> nodes = RefIndexSet.fromRefDistinct(asList(n1, n2, n3));

        JoinComponent path1, path2, path3;
        path1 = new JoinComponent(nodes, n3, n2, n1);
        path2 = new JoinComponent(nodes, n2, n1, n3);
        path3 = new JoinComponent(nodes, n1, n2, n3);

        assertEquals(path1.getNodes(), Sets.newHashSet(n1, n2, n3));
        assertEquals(path2.getNodes(), Sets.newHashSet(n1, n2, n3));
        assertEquals(path3.getNodes(), Sets.newHashSet(n1, n2, n3));

        assertEquals(path1, path2);
        assertEquals(path1, path3);
        assertEquals(path2, path3);
    }

    @Test(groups = {"fast"})
    public static class GroupNodesTest extends GroupNodesTestBase {
        @Override protected @Nonnull List<Op> groupNodes(@Nonnull List<Op> list) {
            return createPathsPlanner().groupNodes(list);
        }
    }

    @DataProvider
    public static Object[][] pathsData() {
        EndpointQueryOp n1 = node(e1, Alice, p1, x);
        EndpointQueryOp n2 = node(e1, x, p1, y);
        EndpointQueryOp n3 = node(e1, Alice, p1, x, x, p1, y);
        EndpointQueryOp n4 = node(e1, y, p1, Bob);
        EndpointQueryOp n5 = node(e1, y, p2, Bob);
        EndpointQueryOp n6 = node(e1, y, p2, x);

        // n*i :: SUBJ is input
        EndpointQueryOp n1i = new EndpointQueryOp(e2, createQuery(
                Alice, AtomInputAnnotation.asRequired(Person, "Person").get(),
                        p1, x, AtomAnnotation.of(Atom1)));
        EndpointQueryOp n2i = new EndpointQueryOp(e2, createQuery(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get(),
                        p1, y, AtomAnnotation.of(Atom1)));
        EndpointQueryOp n4i = new EndpointQueryOp(e2, createQuery(y, AtomInputAnnotation.asRequired(Atom1, "Atom1").get(),
                        p1, Bob, AtomAnnotation.of(Person)));
        EndpointQueryOp n5i = new EndpointQueryOp(e2, createQuery(y, AtomInputAnnotation.asRequired(Atom1, "Atom1").get(),
                        p2, Bob, AtomAnnotation.of(Person)));

        // n*j :: OBJ is input
        EndpointQueryOp n1j = new EndpointQueryOp(e3, createQuery(Alice, AtomAnnotation.of(Person),
                        p1, x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get()));
        EndpointQueryOp n2j = new EndpointQueryOp(e3, createQuery(x, AtomAnnotation.of(Atom1),
                        p1, y, AtomInputAnnotation.asRequired(Atom1, "Atom1").get()));
        EndpointQueryOp n5j = new EndpointQueryOp(e3, createQuery(y, AtomAnnotation.of(Atom1),
                        p2, Bob, AtomInputAnnotation.asRequired(Person, "Person").get()));

        // mXi == M(nX, nXi)
        Op m1i = m(n1, n1i);

        RefIndexSet<Op> nodes = RefIndexSet.fromRefDistinct(
                asList(n1, n2, n3, n4, n5, n6, n1i, n2i, n4i, n5i, n1j, n2j, n5j, m1i));

        return Stream.of(
                asList(createQuery(Alice, p1, x, x, p1, y),
                        asList(n1, n2),
                        singleton(new JoinComponent(nodes, n1, n2))),
                asList(createQuery(Alice, p1, x, x, p1, y),
                        singletonList(n3),
                        singleton(new JoinComponent(nodes, n3))),
                asList(createQuery(Alice, p1, x, x, p1, y),
                        asList(n1, n2, n3),
                        asList(new JoinComponent(nodes, n1, n2),
                               new JoinComponent(nodes, n3))),

                // n1 -> n2 --> n4
                //          +-> n4i
                asList(createQuery(Alice, p1, x, x, p1, y, y, p1, Bob),
                        asList(n1, n2, n4, n4i),
                        asList(new JoinComponent(nodes, n1, n2, n4),
                               new JoinComponent(nodes, n1, n2, n4i))),

                // m1i +-> n2  --+         +--> n5
                //     |         +--> n4 --+
                //     +-> n2i --+         +--> n5i
                //
                asList(createQuery(Alice, p1, x   , x, p1, y  ,
                                   y,     p1, Bob , y, p2, Bob),
                        asList(m1i, n2, n4, n5, n2i, n5i),
                        asList(new JoinComponent(nodes, m1i, n2, n4, n5),
                               new JoinComponent(nodes, m1i, n2, n4, n5i),
                               new JoinComponent(nodes, m1i, n2i, n4, n5),
                               new JoinComponent(nodes, m1i, n2i, n4, n5i)
                        )),

                // n1j <- n2j <--+          +--  n5j
                //         |     |          |
                //   +-----+     +--> n4 <--+
                //   v           |          |
                // n1i -> n2i  --+          +--> n5i
                asList(createQuery(Alice, p1, x  , x, p1, y  ,
                                   y,     p1, Bob, y, p2, Bob),
                        asList(n1j, n2j, n4, n5j, n1i, n2i, n5i),
                        asList(new JoinComponent(nodes, n1i, n2i, n4, n5i),
                               new JoinComponent(nodes, n1i, n2i, n4, n5j),

                               new JoinComponent(nodes, n5j, n4, n2j, n1j),
                               new JoinComponent(nodes, n5j, n4, n2j, n1i),

                               new JoinComponent(nodes, n4, n5i, n2j, n1j),
                               new JoinComponent(nodes, n4, n5i, n2j, n1i)
                        )),

                //  +------+------+
                //  |      |      |
                //  v      v      v
                // n1 <-> n2 <-> n6
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, x),
                       asList(n1, n2, n6),
                       singletonList(new JoinComponent(nodes, n1, n2, n6))),

                //  +------+------+
                //  |      |      |
                //  v      v      |
                // n1j <- n2j <- n6
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, x),
                        asList(n1j, n2j, n6),
                        singletonList(new JoinComponent(nodes, n1j, n2j, n6))),

                //         +-------+
                //  +------|------+|
                //  |      |      ||
                //  v      v      v|
                // n1i -> n2i -> n6i
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, x),
                        asList(n1i, n2i, n6),
                        singletonList(new JoinComponent(nodes, n1i, n2i, n6))),

                //          +----n5j
                //          v
                //  n1j <- n2 -> n5i
                asList(createQuery(Alice, p1, x, x, p1, y, y, p2, Bob),
                       asList(n1j, n2, n5i, n5j),
                       asList(new JoinComponent(nodes, n2, n1j, n5i),
                              new JoinComponent(nodes, n5j, n2, n1j)))
        ).map(List::toArray).toArray(Object[][]::new);
    }


    @Test(dataProvider = "pathsData")
    public void testPaths(CQuery query, List<Op> nodes, Collection<JoinComponent> expectedPaths) {
        double sum = 0;
        int count = 0;
        for (List<Op> permutation : permutations(nodes)) {
            JoinGraph g = new ArrayJoinGraph(RefIndexSet.fromRefDistinct(permutation));
            JoinPathsConjunctivePlanner planner = createPathsPlanner();
            Stopwatch sw = Stopwatch.createStarted();
            List<JoinComponent> paths = planner.getPaths(fromDistinctCopy(query.attr().matchedTriples()), g);
            sum += sw.elapsed(TimeUnit.MICROSECONDS)/1000.0;
            ++count;

            List<JoinComponent> e = emptyList();
//            assertEquals(paths.stream().filter(JoinComponent::hasJoins)
//                    .map(JoinComponent::getJoinInfos)
//                    .filter(this::isBroken).collect(toList()), e);

            HashSet<JoinComponent> exSet = new HashSet<>(expectedPaths);

            assertEquals(paths.stream().filter(p -> !exSet.contains(p)).collect(toList()), e,
                         "There are unexpected paths");
            assertEquals(exSet.stream().filter(p -> !paths.contains(p)).collect(toList()), e,
                         "There are missing paths");
            assertEquals(new HashSet<>(paths), exSet);
        }
        sum /= count;
        System.out.printf("Average ms: %.3f\n", sum);
    }

    @DataProvider
    public static @Nonnull Object[][] indexedSetForDuplicatesData() {
        EndpointQueryOp n1 = node(e1, Alice, p1, x  ), n1a = node(e1a, Alice, p1, x  );
        EndpointQueryOp n2 = node(e1, x,     p2, y  ), n2a = node(e1a, x,     p2, y  );
        EndpointQueryOp n3 = node(e1, y,     p3, z  ), n3a = node(e1a, y,     p3, z  );
        EndpointQueryOp n4 = node(e1, z,     p4, Bob), n4a = node(e1a, z,     p4, Bob);

        EndpointQueryOp n1i = node(e1, b -> b.annotate(Alice, AtomInputAnnotation.asRequired(Person, "Person").get()), Alice, p1, x);
        EndpointQueryOp n2i = node(e1, b -> b.annotate(x, AtomInputAnnotation.asRequired(Person, "Person").get()), x, p2, y);
        EndpointQueryOp n3i = node(e1, b -> b.annotate(y, AtomInputAnnotation.asRequired(Person, "Person").get()), y, p3, z);
        EndpointQueryOp n4i = node(e1, b -> b.annotate(z, AtomInputAnnotation.asRequired(Person, "Person").get()), z, p4, Bob);

        RefIndexSet<Op> all = RefIndexSet.fromRefDistinct(asList(n1 , n2 , n3 , n4 ,
                                                          n1a, n2a, n3a, n4a,
                                                          n1i, n2i, n3i, n4i));

        return Stream.of(
                emptyList(),
                singletonList(new JoinComponent(all, n1, n2, n3, n4)),
                singletonList(new JoinComponent(all, n1a, n2a, n3, n4)),
                asList(new JoinComponent(all, n1, n2),
                       new JoinComponent(all, n1a, n2a)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1a, n2a, n3a)),
                asList(new JoinComponent(all, n1, n2, n3, n4),
                       new JoinComponent(all, n1a, n2a, n3a, n4a)),
                asList(new JoinComponent(all, n1, n2, n3, n4),
                       new JoinComponent(all, n1a, n2a, n3a, n4a),
                       new JoinComponent(all, n1i, n2i, n3i, n4i)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2, n3a)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2, n3i)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2a, n3)),
                asList(new JoinComponent(all, n1, n2, n3),
                       new JoinComponent(all, n1, n2i, n3))
        ).map(l -> new Object[] {l}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "indexedSetForDuplicatesData", groups = {"fast"})
    public void testIndexedSetForDuplicates(List<JoinComponent> paths) {
        assertTrue(paths.stream().noneMatch(Objects::isNull));
        List<JoinComponent> oldPaths = new ArrayList<>(paths);

        JoinPathsConjunctivePlanner planner = createPathsPlanner();
        RefIndexSet<Op> set = planner.getNodesIndexedSetFromPaths(paths);

        //all nodes are in set
        List<Op> missingNonQueryNodes = paths.stream().flatMap(p -> p.getNodes().stream())
                .filter(n -> !(n instanceof EndpointQueryOp) && !set.contains(n)).collect(toList());
        assertEquals(missingNonQueryNodes, emptyList());

        //no changes to the paths themselves
        assertEquals(paths.stream().filter(p -> !oldPaths.contains(p)).collect(toList()),
                     emptyList());

        // no equivalent endpoints for the same query
        for (int i = 0; i < set.size(); i++) {
            if (!(set.get(i) instanceof EndpointQueryOp)) continue;
            EndpointQueryOp outer = (EndpointQueryOp) set.get(i);
            for (int j = i+1; j < set.size(); j++) {
                if (!(set.get(j) instanceof EndpointQueryOp)) continue;
                EndpointQueryOp inner = (EndpointQueryOp) set.get(j);
                if (outer.getQuery().attr().getSet().equals(inner.getQuery().attr().getSet())) {
                    assertFalse(outer.getEndpoint().isAlternative(inner.getEndpoint()));
                    assertFalse(inner.getEndpoint().isAlternative(outer.getEndpoint()));
                }
            }
        }

        // can subset any QueryNode
        List<IndexSubset<Op>> singletons = paths.stream()
                .flatMap(p -> p.getNodes().stream())
                .filter(n -> n instanceof EndpointQueryOp)
                .map(set::subset).collect(toList());
        assertTrue(singletons.stream().noneMatch(IndexSubset::isEmpty));
        assertTrue(singletons.stream().allMatch(s -> s.size() == 1));
    }

    @DataProvider
    public static @Nonnull Object[][] removeAlternativePathsData() {
        EndpointQueryOp n1   = node(e1,  Alice, knows, x);
        EndpointQueryOp n1a  = node(e1a, Alice, knows, x);
        EndpointQueryOp n1b  = node(e2,  Alice, knows, x);
        EndpointQueryOp n1i  = node(e1, b -> b.annotate(x, AtomInputAnnotation.asRequired(Person, "Person").get()),
                                   Alice, knows, x);
        EndpointQueryOp n1ai = node(e1a, b -> b.annotate(x, AtomInputAnnotation.asRequired(Person, "Person").get()),
                                   Alice, knows, x);

        EndpointQueryOp n2   = node(e1,  x, knows, y);
        EndpointQueryOp n2a  = node(e1a, x, knows, y);
        EndpointQueryOp n2i  = node(e1, b -> b.annotate(x, AtomInputAnnotation.asRequired(Person, "Person").get()),
                                   x, knows, y);
        EndpointQueryOp n2ai = node(e1a, b -> b.annotate(x, AtomInputAnnotation.asRequired(Person, "Person").get()),
                                   x, knows, y);


        return Stream.of(
                asList(singletonList(singleton(n1 )), emptyList()),
                asList(singletonList(singleton(n1a)), emptyList()),
                asList(singletonList(asList(n1, n2)), emptyList()),
                asList(asList(asList(n1, n2), asList(n1a, n2)), singletonList(asList(0, 1))),
                asList(asList(asList(n1, n2), asList(n1i, n2)), singletonList(asList(0, 1))),
                asList(asList(asList(n1, n2), asList(n1a, n2), asList(n1i, n2)),
                       singletonList(asList(0, 1, 2))),
                asList(asList(asList(n1, n2), asList(n1a, n2), asList(n1i, n2), asList(n1ai, n2)),
                       singletonList(asList(0, 1, 2, 3))),
                asList(asList(asList(n1, n2), asList(n1b, n2)), emptyList()),
                asList(asList(asList(n1i, n2), asList(n1b, n2)), emptyList()),
                asList(asList(asList(n1a, n2), asList(n1b, n2)), emptyList()),
                asList(asList(asList(n1, n2), asList(n1b, n2), asList(n1ai, n2)),
                       singletonList(asList(0, 2))),
                asList(asList(asList(n1b, n2 ), asList(n1b, n2a ),
                              asList(n1b, n2i), asList(n1b, n2ai)),
                       singletonList(asList(0, 1, 2, 3))),
                asList(asList(asList(n1,  n2i), asList(n1a, n2ai),
                              asList(n1b, n2 ), asList(n1b, n2a )),
                       asList(asList(0, 1), asList(2, 3)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "removeAlternativePathsData", groups = {"fast"})
    public void testRemoveAlternativePaths(List<Collection<Op>> nodesList,
                                           List<List<Integer>> equivIndices) {
        JoinPathsConjunctivePlanner planner = createPathsPlanner();
        //setup
        RefIndexSet<Op> nodes = RefIndexSet.fromRefDistinct(
                nodesList.stream().flatMap(Collection::stream).collect(toSet()));
        JoinGraph graph = new ArrayJoinGraph(nodes);
        List<JoinComponent> pathsList;
        pathsList = nodesList.stream().map(n -> new JoinComponent(graph, n)).collect(toList());
        List<JoinComponent> origPaths = new ArrayList<>(pathsList);

        //sanity
        assertEquals(new HashSet<>(pathsList).size(), nodesList.size());
        for (int i = 0; i < pathsList.size(); i++) {
            // all nodes in path
            assertEquals(pathsList.get(i).getNodes(), new HashSet<>(nodesList.get(i)));

            // alternatives cannot be in the same JoinPath
            List<Op> pathNodes = new ArrayList<>(pathsList.get(i).getNodes());
            for (int j = 0; j < pathNodes.size(); j++) {
                if (!(pathNodes.get(j) instanceof EndpointQueryOp)) continue;
                EndpointQueryOp outer = (EndpointQueryOp) pathNodes.get(j);
                for (int k = j+1; k < pathNodes.size(); k++) {
                    if (!(pathNodes.get(k) instanceof EndpointQueryOp)) continue;
                    EndpointQueryOp inner = (EndpointQueryOp) pathNodes.get(k);
                    if (outer.getQuery().attr().getSet().equals(inner.getQuery().attr().getSet())) {
                        assertFalse(inner.getEndpoint().isAlternative(outer.getEndpoint()));
                        assertFalse(outer.getEndpoint().isAlternative(inner.getEndpoint()));
                    }
                }
            }
        }
        assertTrue(pathsList.stream().noneMatch(Objects::isNull));


        //operation & checks
        planner.removeAlternativePaths(pathsList);
        for (List<Integer> list : equivIndices) {
            Set<JoinComponent> set = list.stream().map(origPaths::get).collect(toSet());
            set.retainAll(pathsList);
            assertEquals(set.size(), 1); // exactly one path must remain
        }

        List<Integer> missing = IntStream.range(0, origPaths.size()).boxed()
                .filter(i -> equivIndices.stream().noneMatch(l -> l.contains(i)))
                .filter(i -> !pathsList.contains(origPaths.get(i)))
                .collect(toList());
        assertEquals(missing, emptyList());
    }

}