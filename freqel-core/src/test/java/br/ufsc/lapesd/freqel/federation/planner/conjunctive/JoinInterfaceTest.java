package br.ufsc.lapesd.freqel.federation.planner.conjunctive;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.JoinInterface;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.freqel.webapis.description.AtomInputAnnotation;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

@Test(groups = {"fast"})
public class JoinInterfaceTest implements TestContext {

    private static final Atom Person = new Atom("Person"), Atom1 = new Atom("Atom1");

    private static final EmptyEndpoint e1 = new EmptyEndpoint();

    private static @Nonnull EndpointQueryOp node(Consumer<MutableCQuery> annotator, Term... terms) {
        MutableCQuery q = new MutableCQuery();
        for (int i = 0; i < terms.length; i += 3)
            q.add(new Triple(terms[i], terms[i + 1], terms[i + 2]));
        annotator.accept(q);
        return new EndpointQueryOp(e1, q);
    }

    private static @Nonnull EndpointQueryOp node(Term... terms) {
        return node(b -> {}, terms);
    }

    private static final EndpointQueryOp n1 = node(Alice, p1, x  ), n2 = node(x, p1, y),
                                   n3 = node(y,     p2, Bob);
    private static final EndpointQueryOp n4 = node(Alice, p1, x, x, p2, y);
    private static final EndpointQueryOp n1i = node(b -> {
                b.annotate(Alice, AtomInputAnnotation.asRequired(Person, "Person").get());
                b.annotate(x, AtomAnnotation.of(Atom1));
            },
            Alice, p1, x);
    private static final EndpointQueryOp n2i = node(b -> {
                b.annotate(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get());
                b.annotate(y, AtomAnnotation.of(Atom1));
            },
            x, p1, y);
    private static final EndpointQueryOp n4i = node(b -> {
                b.annotate(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get());
                b.annotate(y, AtomAnnotation.of(Atom1));
            },
            Alice, p1, x, x, p2, y);
    private static final JoinOp n4j = JoinOp.create(
            node(b -> {
                        b.annotate(Alice, AtomAnnotation.of(Person));
                        b.annotate(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get());
                    },
                 Alice, p1, x),
            node(b -> {
                        b.annotate(x, AtomAnnotation.of(Atom1));
                        b.annotate(y, AtomInputAnnotation.asRequired(Atom1, "Atom1").get());
                    },
                 x, p2, y));
    private static final JoinOp n12j = JoinOp.create(n1, n2);
    private static final JoinOp n12ij = JoinOp.create(n1, n2);


    @DataProvider
    public static Object[][] gettersData() {
        return Stream.of(
                asList(n1, singleton("x"), emptySet(), singleton(new Triple(Alice, p1, x))),
                asList(n2, asList("x", "y"), emptySet(), singleton(new Triple(x, p1, y))),
                asList(n3, singleton("y"), emptySet(), singleton(new Triple(y, p2, Bob))),
                asList(n4, asList("x", "y"), emptySet(),
                        asList(new Triple(Alice, p1, x), new Triple(x, p2, y))),
                asList(n1i, singleton("x"), emptySet(), singleton(new Triple(Alice, p1, x))),
                asList(n2i, asList("x", "y"), singleton("x"), singleton(new Triple(x, p1, y))),
                asList(n4i, asList("x", "y"), singleton("x"),
                        asList(new Triple(Alice, p1, x), new Triple(x, p2, y)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "gettersData")
    public void testGetters(Op node, Collection<String> results, Collection<String> inputs,
                            Collection<Triple> triples) {
        JoinInterface joinInterface = new JoinInterface(node);
        assertEquals(joinInterface.getResultVars(), new HashSet<>(results));
        assertEquals(joinInterface.getMatchedTriples(), new HashSet<>(triples));
        assertEquals(joinInterface.getInputVars(), new HashSet<>(inputs));
    }

    @DataProvider
    public static @Nonnull Object[][] equalsData() {
        return Stream.of(
                asList(n1,   n1,    true),
                asList(n4,   n4,    true),
                asList(n1i,  n1i,   true),
                asList(n1,   n2,    false),
                asList(n1,   n4,    false),
                asList(n2,   n4,    false),
                asList(n1,   n1i,   true),
                asList(n2,   n2i,   false),
                asList(n1i,  n4,    false),
                asList(n2i,  n4i,   false),
                asList(n4i,  n4j,   false),
                asList(n4i,  n12j,  false),
                asList(n4i,  n12ij, false),
                asList(n12j, n12ij, true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "equalsData")
    public void testEquals(Op left, Op right, boolean expected) {
        JoinInterface leftInterface = new JoinInterface(left);
        JoinInterface rightInterface = new JoinInterface(right);
        if (expected)
            assertEquals(leftInterface, rightInterface);
        else
            assertNotEquals(leftInterface, rightInterface);
    }

}