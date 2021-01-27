package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class NoInputStateHelperTest implements TestContext {
    private static final EmptyEndpoint e1 = new EmptyEndpoint(), e2 = new EmptyEndpoint();

    private static @Nonnull EndpointQueryOp n1(Object... args) {
        return new EndpointQueryOp(e1, createQuery(args));
    }

    private static @Nonnull EndpointQueryOp n2(Object... args) {
        return new EndpointQueryOp(e2, createQuery(args));
    }

    private @Nonnull Stream<NoInputStateHelper> helpersStream() {
        RefIndexSet<Op> ops1 = RefIndexSet.fromRefDistinct(asList(
                n1(Alice, knows, x), n1(x, knows, y), n1(y, knows, z)));
        IndexSet<String> vars1 = FullIndexSet.newIndexSet("x", "y", "z");
        IndexSet<Triple> triples1 = FullIndexSet.from(ops1.stream()
                .flatMap(o -> o.getMatchedTriples().stream()).collect(toList()));
        ops1.forEach(o -> o.offerVarsUniverse(vars1));
        ops1.forEach(o -> o.offerTriplesUniverse(triples1));


        RefIndexSet<Op> ops2 = RefIndexSet.fromRefDistinct(asList(
                n1(Alice, knows, x), n1(x, knows, y), n1(y, knows, z, z, knows, Bob)));
        IndexSet<String> vars2 = FullIndexSet.newIndexSet("x", "y", "z", "w", "u", "v");
        IndexSet<Triple> triples2 = FullIndexSet.from(asList(
                new Triple(Alice, age,   u),
                new Triple(Bob,   age,   v),
                new Triple(Alice, knows, x),
                new Triple(x,     knows, y),
                new Triple(y,     knows, z),
                new Triple(z,     knows, Bob),
                new Triple(x,     knows, y)
        ));
        IndexSubset<Triple> query2 = triples2.fullSubset();
        query2.remove(new Triple(Alice, age, u));
        query2.remove(new Triple(Bob,   age, v));
        ops2.forEach(o -> o.offerVarsUniverse(vars2));
        ops2.forEach(o -> o.offerTriplesUniverse(triples2));

        RefIndexSet<Op> ops3 = RefIndexSet.fromRefDistinct(asList(
                n1(Alice, knows, x), n1(x, knows, y), n1(y, knows, z, z, knows, Bob),
                n2(Bob, age, v)));
        IndexSet<String> vars3 = FullIndexSet.newIndexSet("v", "x", "y", "z", "w", "u");
        IndexSet<Triple> triples3 = FullIndexSet.from(asList(
                new Triple(Alice, age,   u),
                new Triple(Bob,   age,   v),
                new Triple(Alice, knows, x),
                new Triple(x,     knows, y),
                new Triple(y,     knows, z),
                new Triple(z,     knows, Bob),
                new Triple(x,     knows, y)
        ));
        IndexSubset<Triple> query3 = triples3.fullSubset();
        query3.remove(new Triple(Alice, age, u));
        query3.remove(new Triple(Bob,   age, v));
        ops3.forEach(o -> o.offerVarsUniverse(vars3));
        ops3.forEach(o -> o.offerTriplesUniverse(triples3));

        return Stream.of(
                new NoInputStateHelper(new BitJoinGraph(ops1), vars1, triples1.fullSubset()),
                new NoInputStateHelper(new BitJoinGraph(ops2), vars2, query2),
                new NoInputStateHelper(new BitJoinGraph(ops3), vars3, query3)
        );
    }

    @DataProvider public @Nonnull Object[][] helperData() {
        return helpersStream().map(h -> new Object[] {h}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "helperData")
    public void testIsFinal(@Nonnull NoInputStateHelper helper) {
        RefIndexSet<Op> nodes = helper.graph.getNodes();
        for (Set<Op> subset : Sets.powerSet(nodes)) {
            Set<Triple> observed = subset.stream().flatMap(o -> o.getMatchedTriples().stream())
                    .collect(toSet());
            IndexSubset<Triple> queryTriples = helper.triples.subset(helper.queryTriples);
            if (!queryTriples.containsAll(observed))
                continue; //ignore, since this is not a valid state
            boolean expected = observed.containsAll(queryTriples);
            long[] state = helper.createState();
            helper.bs.or(state, 0, nodes.subset(subset).getBitset().toLongArray());
            helper.bs.or(state, 1, helper.vars.subset(subset.stream()
                    .flatMap(o -> o.getAllVars().stream()).collect(toList()))
                    .getBitset().toLongArray());
            helper.bs.or(state, 2, helper.triples.subset(observed).getBitset().toLongArray());
            assertEquals(helper.isFinal(state), expected);
        }
    }

    @Test
    public void testAddNode() {
        NoInputStateHelper helper = helpersStream().collect(toList()).get(2);
        IndexSet<String> vars = helper.vars;

        long[] state = helper.createState(0);
        helper.bs.or(state, 1, vars.subset("x").getBitset().toLongArray());
        helper.bs.set(state, 2, 2);
        long[] copy = Arrays.copyOf(state, state.length);
        assertEquals(state, copy);

        assertNull(helper.addNode(state, 0)); //node already present
        assertEquals(state, copy);
        assertThrows(AssertionError.class, () -> helper.addNode(state, 2)); //no intersection
        assertEquals(state, copy);

        long[] next = helper.addNode(state, 1);
        assertNotNull(next);
        assertEquals(state, copy);

        assertTrue(helper.bs.get(next, 0, 0));
        assertTrue(helper.bs.get(next, 0, 1));
        assertEquals(helper.bs.cardinality(next, 0), 2);

        assertTrue(helper.bs.equals(next, 1,
                   vars.subset(asList("x", "y")).getBitset().toLongArray()));
        assertTrue(helper.bs.get(next, 2, 2));
        assertTrue(helper.bs.get(next, 2, 3));
        assertEquals(helper.bs.cardinality(next, 2), 2);
    }
}