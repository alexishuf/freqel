package br.ufsc.lapesd.freqel.federation.planner.conjunctive;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomAnnotation;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import com.google.common.base.Preconditions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Collections2.permutations;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public abstract class GroupNodesTestBase implements TestContext {
    private static final Atom Person = new Atom("Person"), Atom1 = new Atom("Atom1");

    private static final EmptyEndpoint e1 = new EmptyEndpoint("e1"), e1a = new EmptyEndpoint("e1a"),
            e2 = new EmptyEndpoint("e2"), e3  = new EmptyEndpoint("e3");

    static {
        e1.addAlternative(e1a);
    }

    private static  @Nonnull
    EndpointQueryOp node(CQEndpoint ep, @Nonnull java.util.function.Consumer<MutableCQuery> setup,
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

    @DataProvider
    public static Object[][] groupNodesData() {
        EndpointQueryOp n1  = node(e1,  Alice, p1, x), n2 = node(e1, x, p2, y), n3 = node(e1, y, p3, Bob);
        EndpointQueryOp o1  = node(e2,  Alice, p1, x), o2 = node(e2, x, p2, y), o3 = node(e2, y, p3, Bob);
        EndpointQueryOp o1a = node(e1a, Alice, p1, x);
        EndpointQueryOp i2 = new EndpointQueryOp(e2, createQuery(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get(),
                p2, y, AtomAnnotation.of(Atom1)));
        EndpointQueryOp aliceKnowsX = node(e1, Alice, knows, x), yKnowsBob = node(e1, y, knows, Bob);

        return Stream.of(
                asList(singleton(n1), singleton(n1)),
                asList(asList(n1, n2, n3), asList(n1, n2, n3)),
                asList(asList(n1, n2, o1), asList(m(n1, o1), n2)),
                asList(asList(n1, n2, o1a), asList(m(n1, o1a), n2)), //(n1, o1a) are alternatives
                asList(asList(n2, i2), asList(n2, i2)),
                asList(asList(n1, n2, i2), asList(n1, n2, i2)),
                asList(asList(n2, o2, i2), asList(m(n2, o2), i2)),
                asList(asList(n2, o2, i2, n3), asList(m(n2, o2), i2, n3)),
                asList(asList(n1, o1, n2, o2, i2, n3), asList(m(n1, o1), m(n2, o2), i2, n3)),
                asList(asList(n1, o1, n2, o2, i2, n3, o3),
                        asList(m(n1,o1), m(n2,o2), i2, m(n3,o3))),
                asList(asList(aliceKnowsX, yKnowsBob), asList(aliceKnowsX, yKnowsBob))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    private static boolean nodeAlternativeMatch(@Nonnull Op actual, @Nonnull Op expected) {
        TPEndpoint acEndpoint = ((EndpointOp) actual  ).getEndpoint();
        TPEndpoint exEndpoint = ((EndpointOp) expected).getEndpoint();
        if (!acEndpoint.isAlternative(exEndpoint)) return false;
        if (!exEndpoint.isAlternative(acEndpoint)) return false;
        if (expected instanceof EndpointQueryOp) {
            if (!(actual instanceof EndpointQueryOp)) return false;
            MutableCQuery acQuery = ((EndpointQueryOp) actual).getQuery();
            MutableCQuery exQuery = ((EndpointQueryOp) expected).getQuery();
            return acQuery.equals(exQuery);
        } else if (expected instanceof DQueryOp) {
            if (!(actual instanceof DQueryOp)) return false;
            Op acQuery = ((DQueryOp) actual).getQuery();
            Op exQuery = ((DQueryOp) expected).getQuery();
            return acQuery.equals(exQuery);
        }
        return true;
    }

    private static boolean nodeMatch(@Nonnull Op actual, @Nonnull Op expected,
                                     boolean removeAlternatives) {
        if (expected instanceof UnionOp) {
            if (!removeAlternatives) {
                if (!(actual instanceof UnionOp)) return false;
                if (expected.getChildren().size() != actual.getChildren().size()) return false;
                return expected.getChildren().stream()
                        .anyMatch(e -> actual.getChildren().stream()
                                .anyMatch(a -> nodeMatch(a, e, removeAlternatives)));
            }
            List<Op> aChildren = actual instanceof UnionOp ? actual.getChildren()
                                                           : singletonList(actual);
            List<Op> missing = expected.getChildren().stream()
                                       .filter(c -> aChildren.stream().noneMatch(c::equals))
                                       .collect(toList());
            // every missing Op has an Op with same query and an alternative TPEndpoint
            return missing.stream().allMatch(e -> aChildren.stream()
                              .anyMatch(a -> nodeAlternativeMatch(a, e)));
        }
        boolean eq = actual.equals(expected);
        if (eq) {
            assertTrue(expected instanceof EndpointOp);
            assertSame(((EndpointOp) actual).getEndpoint(),
                       ((EndpointOp) expected).getEndpoint());
            if (expected instanceof EndpointQueryOp) {
                assertTrue(actual instanceof EndpointQueryOp);
                assertEquals(((EndpointQueryOp) actual).getQuery(),
                             ((EndpointQueryOp) expected).getQuery());
            } else if (expected instanceof DQueryOp) {
                assertTrue(actual instanceof DQueryOp);
                assertEquals(((DQueryOp) actual).getQuery(),
                             ((DQueryOp) expected).getQuery());
            }
        }
        return eq;
    }

    public static class SkipGroupNodesException extends RuntimeException {

    }

    protected abstract @Nonnull List<Op> groupNodes(@Nonnull List<Op> list);

    protected boolean removesAlternatives() {
        return false;
    }

    @Test(dataProvider = "groupNodesData", groups = {"fast"})
    public void testGroupNodes(Collection<Op> in, Collection<Op> expected) {
        try {
            //noinspection UnstableApiUsage
            for (List<Op> permutation : permutations(in)) {
                List<Op> grouped = groupNodes(permutation);
                for (Op op : grouped) {
                    if (op instanceof UnionOp) {
                        // all children must be EndpointQueryOp/DQueryOps
                        List<Op> cs = op.getChildren();
                        if (cs.stream().anyMatch(EndpointQueryOp.class::isInstance))
                            assertTrue(cs.stream().allMatch(EndpointQueryOp.class::isInstance));
                        else
                            assertTrue(cs.stream().allMatch(DQueryOp.class::isInstance));
                        // all children must match the same triples and have the same inputs
                        assertEquals(cs.stream().map(Op::getMatchedTriples).distinct().count(), 1);
                        assertEquals(cs.stream().map(Op::getInputVars).distinct().count(), 1);
                    }
                }
                assertTrue(grouped.size() <= expected.size());
                boolean rmAlts = removesAlternatives();
                if (!rmAlts)
                    assertEquals(grouped.size(), expected.size());
                for (Op ex : expected) {
                    assertTrue(grouped.stream().anyMatch(ac -> nodeMatch(ac, ex, rmAlts)),
                               "No match for " + ex);
                }
            }
        } catch (SkipGroupNodesException ignored) { /*ignore*/ }
    }
}
