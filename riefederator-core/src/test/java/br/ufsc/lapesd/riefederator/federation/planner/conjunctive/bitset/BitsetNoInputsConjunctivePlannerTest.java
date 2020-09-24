package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.GroupNodesTestBase;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.util.RawAlignedBitSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerTest.withUniverseSets;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class BitsetNoInputsConjunctivePlannerTest implements TestContext {
    @Test(groups = {"fast"})
    public static class GroupNodesNoInputTest extends GroupNodesTestBase {

        @SuppressWarnings("TestNGDataProvider")
        @Test(dataProvider = "groupNodesData", groups = {"fast"})
        public void testAddUniverses(Collection<Op> in, Collection<Op> expected) {
            doTestAddUniverses(in);
            doTestAddUniverses(expected);
        }

        private void doTestAddUniverses(Collection<Op> in) {
            List<Op> copies = withUniverseSets(in);
            assertEquals(copies.size(), in.size());
            IndexSet<String> vars = null;
            IndexSet<Triple> triples = null;
            for (Op copy : copies) {
                assertTrue(in.stream().anyMatch(copy::equals));
                assertTrue(copy.getAllVars()           instanceof IndexSubset);
                assertTrue(copy.getResultVars()        instanceof IndexSubset);
                assertTrue(copy.getStrictResultVars()  instanceof IndexSubset);
                assertTrue(copy.getRequiredInputVars() instanceof IndexSubset);
                assertTrue(copy.getOptionalInputVars() instanceof IndexSubset);
                assertTrue(copy.getInputVars()         instanceof IndexSubset);
                assertTrue(copy.getPublicVars()        instanceof IndexSubset);

                IndexSet<String> varsParent = ((IndexSubset<String>) copy.getAllVars()).getParent();
                if (vars == null) vars = varsParent;
                else              assertSame(varsParent, vars);
                assertSame(((IndexSubset<String>) copy.getResultVars()).getParent(),        vars);
                assertSame(((IndexSubset<String>) copy.getStrictResultVars()).getParent(),  vars);
                assertSame(((IndexSubset<String>) copy.getRequiredInputVars()).getParent(), vars);
                assertSame(((IndexSubset<String>) copy.getOptionalInputVars()).getParent(), vars);
                assertSame(((IndexSubset<String>) copy.getInputVars()).getParent(),         vars);
                assertSame(((IndexSubset<String>) copy.getPublicVars()).getParent(),        vars);

                IndexSet<Triple> triplesParent =
                        ((IndexSubset<Triple>) copy.getMatchedTriples()).getParent();
                if (triples == null) triples = triplesParent;
                else                 assertSame(triplesParent, triples);
            }
            //Every copy is equal to a input, confirm the input order was preserved
            assertEquals(copies, new ArrayList<>(in));
        }

        @Override protected boolean removesAlternatives() {
            return true;
        }

        @Override protected @Nonnull List<Op> groupNodes(@Nonnull List<Op> list) {
            if (list.stream().anyMatch(Op::hasInputs))
                throw new SkipGroupNodesException();
            BitsetNoInputsConjunctivePlanner planner = SingletonSourceFederation.getInjector()
                    .getInstance(BitsetNoInputsConjunctivePlanner.class);
            return planner.groupNodes(withUniverseSets(list));
        }
    }

    private static @Nonnull BitSet bs(int... values) {
        BitSet bs = new BitSet();
        for (int i : values) bs.set(i);
        return bs;
    }

    @DataProvider public @Nonnull Object[][] testFindCommonSubsetsData() {
        return Stream.of(
                asList(asList(bs()), emptyList()),
                asList(asList(bs(1, 2)), emptyList()),
                asList(asList(bs(1, 2), bs(3, 4)), emptyList()),
                asList(asList(bs(1, 2), bs(2, 3)), emptyList()),
                asList(asList(bs(1, 2), bs(2, 3), bs(2, 4)), emptyList()),
                asList(asList(bs(1, 2, 3), bs(2, 3, 4)), asList(bs(2, 3))),
                asList(asList(bs(1, 2), bs(2, 3, 4), bs(2, 3, 5)), asList(bs(2, 3))),

                /* same as above, but force long[] version to be used */
                asList(asList(bs(101, 102)), emptyList()),
                asList(asList(bs(101, 102), bs(103, 104)), emptyList()),
                asList(asList(bs(101, 102), bs(102, 103)), emptyList()),
                asList(asList(bs(101, 102), bs(102, 103), bs(102, 104)), emptyList()),
                asList(asList(bs(101, 102, 103), bs(102, 103, 104)), asList(bs(102, 103))),
                asList(asList(bs(101, 102), bs(102, 103, 104), bs(102, 103, 105)), asList(bs(102, 103)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testFindCommonSubsetsData")
    public void testFindCommonSubsets(@Nonnull List<BitSet> components,
                                      @Nonnull Collection<BitSet> expected) {
        int nNodes = components.stream().map(BitSet::length).max(Integer::compareTo).orElse(-1);
        RawAlignedBitSet bs = new RawAlignedBitSet(nNodes, 8, 8);
        List<long[]> states = new ArrayList<>(components.size());
        for (BitSet component : components) {
            long[] state = bs.alloc();
            long[] temp = component.toLongArray();
            assertTrue(temp.length < state.length);
            System.arraycopy(temp, 0, state, 0, temp.length);
            states.add(state);
        }

        List<Op> graphNodesList = new ArrayList<>();
        for (int i = 0; i < nNodes; i++) {
            StdVar s = new StdVar("s" + "-" + i);
            StdURI p = new StdURI(EX + "p" + "-" + i);
            StdVar o = new StdVar("o" + "-" + i);
            graphNodesList.add(new QueryOp(createQuery(s, p, o)));
        }
        RefIndexSet<Op> graphNodes = RefIndexSet.fromRefDistinct(withUniverseSets(graphNodesList));
        BitJoinGraph dummyGraph = new BitJoinGraph(graphNodes);

        BitsetNoInputsConjunctivePlanner planner = SingletonSourceFederation.getInjector()
                .getInstance(BitsetNoInputsConjunctivePlanner.class);
        List<BitSet> list = planner.findCommonSubsets(states, dummyGraph);
        assertEquals(new HashSet<BitSet>(list), new HashSet<>(expected));
    }

    @Test(groups = {"fast"})
    public static class PlanBenchmarksTest extends ConjunctivePlanBenchmarksTestBase {
        @Override protected @Nonnull Class<? extends ConjunctivePlanner> getPlannerClass() {
            return BitsetNoInputsConjunctivePlanner.class;
        }
    }
}