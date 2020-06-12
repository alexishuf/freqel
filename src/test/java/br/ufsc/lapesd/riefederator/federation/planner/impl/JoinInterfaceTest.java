package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
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

    private static @Nonnull QueryNode node(Consumer<CQuery.Builder> annotator, Term... terms) {
        CQuery.Builder b = CQuery.builder();
        for (int i = 0; i < terms.length; i += 3)
            b.add(new Triple(terms[i], terms[i + 1], terms[i + 2]));
        annotator.accept(b);
        return new QueryNode(e1, b.build());
    }

    private static @Nonnull QueryNode node(Term... terms) {
        return node(b -> {}, terms);
    }

    private static QueryNode n1 = node(Alice, p1, x), n2 = node(x, p1, y), n3 = node(y, p2, Bob);
    private static QueryNode n4 = node(Alice, p1, x, x, p2, y);
    private static QueryNode n1i = node(b -> b.annotate(Alice, AtomInputAnnotation.asRequired(Person, "Person").get())
                                              .annotate(x, AtomAnnotation.of(Atom1)),
            Alice, p1, x);
    private static QueryNode n2i = node(b -> b.annotate(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get())
                                              .annotate(y, AtomAnnotation.of(Atom1)),
            x, p1, y);
    private static QueryNode n4i = node(b -> b.annotate(x, AtomInputAnnotation.asRequired(Atom1, "Atom1").get())
                                              .annotate(y, AtomAnnotation.of(Atom1)),
            Alice, p1, x, x, p2, y);
    private static JoinNode n4j = JoinNode.builder(
            node(b -> b.annotate(Alice, AtomAnnotation.of(Person))
                       .annotate(x,     AtomInputAnnotation.asRequired(Atom1, "Atom1").get()),
                 Alice, p1, x),
            node(b -> b.annotate(x, AtomAnnotation.of(Atom1))
                       .annotate(y, AtomInputAnnotation.asRequired(Atom1, "Atom1").get()),
                 x, p2, y)).build();
    private static JoinNode n12j = JoinNode.builder(n1, n2).build();
    private static JoinNode n12ij = JoinNode.builder(n1, n2).build();


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
    public void testGetters(PlanNode node, Collection<String> results, Collection<String> inputs,
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
    public void testEquals(PlanNode left, PlanNode right, boolean expected) {
        JoinInterface leftInterface = new JoinInterface(left);
        JoinInterface rightInterface = new JoinInterface(right);
        if (expected)
            assertEquals(leftInterface, rightInterface);
        else
            assertNotEquals(leftInterface, rightInterface);
    }

}