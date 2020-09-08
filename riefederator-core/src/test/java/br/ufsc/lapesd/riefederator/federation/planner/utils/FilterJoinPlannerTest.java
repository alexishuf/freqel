package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.inject.Injector;
import org.apache.jena.rdf.model.ModelFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.Cardinality.guess;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public class FilterJoinPlannerTest implements TestContext {
    private static final CQEndpoint ep = ARQEndpoint.forModel(ModelFactory.createDefaultModel());
    private static final Injector injector = SingletonSourceFederation.getInjector();

    public static final @Nonnull List<Supplier<FilterJoinPlanner>> suppliers = asList(
            DefaultFilterJoinPlannerTest::createDefault,
            () -> {
                JoinOrderPlanner joPlanner = injector.getInstance(GreedyJoinOrderPlanner.class);
                CardinalityComparator cardComparator = ThresholdCardinalityComparator.DEFAULT;
                return new DefaultFilterJoinPlanner(cardComparator, joPlanner);
            },
            () -> injector.getInstance(DefaultFilterJoinPlanner.class)
    );

    private static @Nonnull EndpointQueryOp q(Cardinality cardinality, Object... args) {
        EndpointQueryOp op = new EndpointQueryOp(ep, createQuery(args));
        op.setCardinality(cardinality);
        return op;
    }

    private static @Nonnull EndpointQueryOp q(Object... args) {
        return q(Cardinality.UNSUPPORTED, args);
    }

    private static boolean checkJoin(@Nonnull Op root, int nodes, @Nonnull CQuery base) {
        if (TreeUtils.streamPreOrder(root).count() != nodes)
            return false;
        if (!(root instanceof JoinOp))
            return false;
        if (!root.getMatchedTriples().equals(base.attr().getSet()))
            return false;
        List<Set<Triple>> mts = TreeUtils.streamPreOrder(root).filter(o -> !(o instanceof JoinOp))
                                         .map(Op::getMatchedTriples).collect(toList());
        for (int i = 0, size = mts.size(); i < size; i++) {
            Set<Triple> outer = mts.get(i);
            for (int j = 0; j < size; j++) {
                if (i != j && mts.get(j).stream().anyMatch(outer::contains))
                    return false; // no overlap allowed
            }
        }
        for (SPARQLFilter filter : base.getModifiers().filters()) {
            long c = TreeUtils.streamPreOrder(root)
                              .filter(o -> o.modifiers().contains(filter)).count();
            if (c != 1)
                return false;
        }
        return true;
    }

    @DataProvider
    public static @Nonnull Object[][] testCartesianData() {
        return Stream.of(
                // no filters in cartesian, no change
                asList(CartesianOp.builder().add(q(Alice, name, x)).add(q(Alice, age, u)).build(),
                       emptySet(), null),
                // no filter on parent enables joining
                asList(CartesianOp.builder()
                                .add(q(Alice, knows, x))
                                .add(q(Alice, age, u, Alice, age, v))
                                .add(SPARQLFilter.build("?u < 23"))
                                .add(SPARQLFilter.build("?u > ?v")).build(),
                       emptySet(), null),
                // simple joining filter
                asList(CartesianOp.builder()
                                .add(q(guess(10), Alice, age, u))
                                .add(q(guess(20), Alice, ageEx, v))
                                .add(SPARQLFilter.build("?u > ?v")).build(),
                       emptySet(),
                       (Predicate<Op>) o -> checkJoin(o, 3,
                               createQuery(Alice, age, u, Alice, ageEx, v,
                                           SPARQLFilter.build("?u > ?v")))),
                // joining filter fed by two nodes
                asList(CartesianOp.builder()
                                .add(q(guess(10), Alice, age, u))
                                .add(q(guess(20), Alice, ageEx, v))
                                .add(q(guess(30), Alice, p1, w))
                                .add(SPARQLFilter.build("?u < ?v && ?v < ?w")).build(),
                       emptySet(), (Predicate<Op>)o -> checkJoin(o, 5,
                                createQuery(Alice, age, u, Alice, ageEx, v, Alice, p1, w,
                                            SPARQLFilter.build("?u < ?v"),
                                            SPARQLFilter.build("?v < ?w")))),

                // produce a product of two joins
                asList(CartesianOp.builder()
                                .add(q(guess(10), Alice, age, u))
                                .add(q(guess(10), Alice, ageEx, v))
                                .add(q(guess(10), Alice, p1, o1))
                                .add(q(guess(10), Alice, p2, o2))
                                .add(SPARQLFilter.build("?u < ?v && str(?o1) = str(?o2)")).build(),
                       emptySet(), (Predicate<Op>)o -> {
                            boolean ok = o instanceof CartesianOp;
                            ok &= o.getChildren().size() == 2;
                            Op fst = o.getChildren().stream().filter(c -> c.getResultVars()
                                            .contains("u")).findFirst().orElse(null);
                            ok &= fst != null;
                            if (fst != null) {
                                ok &= checkJoin(fst, 3,
                                                createQuery(Alice, age, u, Alice, ageEx, v,
                                                            SPARQLFilter.build("?u < ?v")));
                            }
                            Op snd = o.getChildren().stream().filter(c -> c.getResultVars()
                                            .contains("o1")).findFirst().orElse(null);
                            ok &= snd != null;
                            if (snd != null) {
                                ok &= checkJoin(snd, 3, createQuery(
                                        Alice, p1, o1, Alice, p2, o2,
                                        SPARQLFilter.build("str(?o1) = str(?o2)")));
                            }
                            ok &= TreeUtils.streamPreOrder(o)
                                    .filter(n -> !(n instanceof EndpointQueryOp))
                                    .allMatch(n -> n.modifiers().filters().isEmpty());
                            return ok;
                        })
        ).flatMap(l -> suppliers.stream().map(s -> {
            ArrayList<Object> copy = new ArrayList<>(l);
            copy.add(0, s);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testCartesianData")
    public void testCartesian(@Nonnull Supplier<FilterJoinPlanner> supplier,
                              @Nonnull CartesianOp unsafeIn, @Nonnull Set<RefEquals<Op>> shared,
                              @Nullable Predicate<Op> checker) {
        CartesianOp in = (CartesianOp) TreeUtils.deepCopy(unsafeIn);
        if (checker == null) {
            Op copy = TreeUtils.deepCopy(in);
            checker = o -> Objects.equals(o, copy) && o == in;
        }
        Op actual = supplier.get().rewrite(in, shared);
        ConjunctivePlannerTest.assertPlanAnswers(actual, unsafeIn);
        assertTrue(checker.test(actual));
    }

    @DataProvider
    public static @Nonnull Object[][] testJoinData() {
        return Stream.of(
                // no filters, no change
                asList(JoinOp.create(q(x, name, u), q(x, age, u)), emptySet(), null),
                // filters on leaves do not trigger
                asList(JoinOp.create(q(x, age, u, SPARQLFilter.build("?u < 23")),
                                     q(x, age, v)), emptySet(), null),
                // no effect if leaf has join filter
                asList(JoinOp.create(q(x, age, u), q(x, age, v, SPARQLFilter.build("?v > ?u"))),
                       emptySet(), null),
                // push filter to leaf that already has inputs
                asList(JoinOp.builder(q(x, age, u),
                                     q(x, age, v, SPARQLFilter.build("?v < ?w")))
                             .add(SPARQLFilter.build("?u < ?v")).build(),
                       emptySet(),
                       JoinOp.create(q(x, age, u), q(x, age, v, SPARQLFilter.build("?v < ?w"),
                                                                SPARQLFilter.build("?u < ?v"))))
        ).flatMap(l -> suppliers.stream().map(s -> {
            ArrayList<Object> copy = new ArrayList<>(l);
            copy.add(0, s);
            return copy;
        })).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testJoinData")
    public void testJoin(@Nonnull Supplier<FilterJoinPlanner> supplier,
                         @Nonnull JoinOp unsafeIn, @Nonnull Set<RefEquals<Op>> shared,
                         @Nullable JoinOp expected) {
        if (expected == null)
            expected = unsafeIn;
        boolean expectSame = expected == unsafeIn;
        expected = (JoinOp) TreeUtils.deepCopy(expected);
        JoinOp in = (JoinOp) TreeUtils.deepCopy(unsafeIn);
        Op actual = supplier.get().rewrite(in, shared);
        assertTrue(actual instanceof JoinOp);
        assertEquals(actual, expected);
        ConjunctivePlannerTest.assertPlanAnswers(actual, unsafeIn);
        if (expectSame)
            assertSame(actual, in);
    }

}