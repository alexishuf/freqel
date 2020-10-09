package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.InputsBitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.CQueryCache;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import br.ufsc.lapesd.riefederator.util.ref.IdentityHashSet;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Collections2;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.AbstractStateHelper.*;
import static br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.InputStateHelper.INPUTS;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static org.testng.Assert.*;

public class InputStateHelperTest implements TestContext {

    @DataProvider public @Nonnull Object[][] queryData() {
        List<List<Object>> rows = new ArrayList<>();
        for (int count = 2; count < 8; count++) {
            List<Op> nodes = new ArrayList<>(count);
            Map<Integer, Atom> atoms = new HashMap<>();
            for (int i = 0; i < count; i++) {
                StdVar s = new StdVar("x"+i), o = new StdVar("x"+(i+1));
                StdURI p = new StdURI(EX + "p" + i);
                MutableCQuery q = createQuery(s, p, o);
                if (i > 0) {
                    Atom atom = atoms.computeIfAbsent(i, k -> new Atom("A"+k));
                    q.annotate(s, AtomInputAnnotation.asRequired(atom, "in"+i).get());
                }
                nodes.add(new EndpointQueryOp(new EmptyEndpoint(), q));
            }
            nodes = BitsetConjunctivePlannerTest.withUniverseSets(nodes);
            for (int i = 0; i < count; i++) {
                MutableCQuery query = new MutableCQuery();
                for (int j = 0; j <= i; j++)
                    query.addAll(nodes.get(j).getMatchedTriples());
                Op n = nodes.get(0);
                CQueryCache c = query.attr();
                c.offerTriplesUniverse(((IndexSubset<Triple>)n.getMatchedTriples()).getParent());
                c.offerVarNamesUniverse(((IndexSubset<String>)n.getAllVars()).getParent());
                rows.add(asList(query, nodes));
            }
        }
        return rows.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "queryData")
    public void testAddNode(@Nonnull CQuery query, List<Op> fragments) {
        BitJoinGraph graph = new InputsBitJoinGraph(RefIndexSet.fromRefDistinct(fragments));
        IndexSet<String> vars = ((IndexSubset<String>) fragments.get(0).getAllVars()).getParent();
        IndexSubset<Triple> queryTriples = (IndexSubset<Triple>) query.attr().getSet();
        InputStateHelper helper = new InputStateHelper(graph, vars, queryTriples);

        //noinspection UnstableApiUsage
        for (List<Op> permutation : Collections2.permutations(fragments)) {
            long[] state = helper.createState();
            State expected = new State();
            for (Op op : permutation) {
                if (!((IndexSubset<String>)op.getPublicVars()).containsAny(expected.vars)) {
                    long[] finalState = state;
                    assertThrows(AssertionError.class,
                                 () -> helper.addNode(finalState, graph.indexOf(op)));
                } else {
                    long[] next = helper.addNode(state, graph.indexOf(op));
                    assertEquals(next != null, expected.addNode(op));
                    if (next != null) {
                        assertTrue(expected.matches(helper, next));
                        boolean expectedFinal = expected.isFinal(query);
                        assertEquals(helper.isFinal(state), expectedFinal);
                        state = next;
                    }
                }
            }
        }
    }

    private class State {
        private IdentityHashSet<Op> nodes;
        private Set<String> vars, reqInputVars;
        private Set<Triple> triples;

        public State() {
            nodes = new IdentityHashSet<>();
            vars = new HashSet<>();
            reqInputVars = new HashSet<>();
            triples = new HashSet<>();
        }

        public boolean isFinal(@Nonnull CQuery query) {
            return triples.equals(query.attr().getSet()) && reqInputVars.isEmpty();
        }

        public boolean matches(@Nonnull InputStateHelper helper, @Nonnull long[] state) {
            IndexSubset<Op> nodes = helper.nodes.subset(helper.bs.asBitset(state, NODES));
            IndexSubset<String> vars = helper.vars.subset(helper.bs.asBitset(state, VARS));
            IndexSubset<String> reqInputVars = helper.vars.subset(helper.bs.asBitset(state, INPUTS));
            IndexSubset<Triple> triples = helper.triples.subset(helper.bs.asBitset(state, TRIPLES));

            if (!this.nodes.equals(nodes)) return false;
            if (!this.vars.equals(vars)) return false;
            if (!this.reqInputVars.equals(reqInputVars)) return false;
            if (!this.triples.equals(triples)) return false;
            return true;
        }

        public boolean addNode(@Nonnull Op node) {
            if (nodes.contains(node))
                return false;
            HashSet<String> joinVars = new HashSet<>(node.getPublicVars());
            joinVars.retainAll(vars);
            if (joinVars.isEmpty())
                return false;
            HashSet<String> sharedReqInputs = new HashSet<>(reqInputVars);
            sharedReqInputs.retainAll(node.getRequiredInputVars());
            joinVars.removeAll(sharedReqInputs);
            if (joinVars.isEmpty())
                return false;
            if (node.getMatchedTriples().containsAll(triples))
                return false;
            if (triples.containsAll(node.getMatchedTriples()))
                return false;

            // incorporate node
            nodes.add(node);
            triples.addAll(node.getMatchedTriples());
            vars.addAll(node.getPublicVars());

            HashSet<String> outVars = new HashSet<>(node.getPublicVars());
            outVars.removeAll(node.getRequiredInputVars());
            reqInputVars.removeAll(outVars);

            HashSet<String> inVars = new HashSet<>(node.getRequiredInputVars());
            HashSet<String> myOutVars = new HashSet<>(vars);
            myOutVars.removeAll(reqInputVars);
            inVars.removeAll(myOutVars);
            reqInputVars.addAll(inVars);
            return true;
        }
    }
}