package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.RawAlignedBitSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;

class NoInputStateHelper extends AbstractStateHelper {
    public NoInputStateHelper(@Nonnull BitJoinGraph graph, @Nonnull IndexSet<String> vars,
                              @Nonnull IndexSubset<Triple> queryTriples) {
        super(graph, vars, queryTriples,
              new RawAlignedBitSet(graph.getNodes().size(), vars.size(),
                                   queryTriples.getParent().size()));
    }

    public @Nonnull long[] createState(int nodeIdx) {
        long[] data = bs.alloc(), tmp;
        bs.set(data, NODES, nodeIdx);
        Op node = nodes.get(nodeIdx);
        tmp = ((IndexSubset<String>)node.getPublicVars()).getBitSet().toLongArray();
        bs.or(data, VARS, tmp);
        tmp = ((IndexSubset<Triple>)node.getMatchedTriples()).getBitSet().toLongArray();
        bs.or(data, TRIPLES, tmp);
        return data;
    }

    public @Nullable long[] addNode(@Nonnull long[] state, int nodeIdx) {
        if (bs.get(state, NODES, nodeIdx))
            return null; // node already in state
        Op n = graph.get(nodeIdx);
        assert ((IndexSubset<String>)n.getResultVars()).getParent() == vars;
        assert ((IndexSubset<Triple>) n.getMatchedTriples()).getParent() == triples;

        long[] nodeVars = ((IndexSubset<String>) n.getResultVars()).getBitSet().toLongArray();
        assert bs.intersects(nodeVars, state, VARS) : "New node shares no variable with state";

        BitSet nodeTriplesBS = ((IndexSubset<Triple>) n.getMatchedTriples()).getBitSet();
        long[] nodeTriples = nodeTriplesBS.toLongArray();
        if (bs.containsAll(nodeTriples, state, TRIPLES)
                || bs.containsAll(state, TRIPLES, nodeTriples)) {
            return null; //one triple set subsumes the other
        }

        // check if any old node is subsumed by all other old nodes and the new node
        int nNodes = 0;
        BitSet[] sets = new BitSet[graph.size()];
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i+1)) 
            sets[nNodes++] = ((IndexSubset<Triple>)graph.get(i).getMatchedTriples()).getBitSet();
        for (int i = 0; i < nNodes; i++) {
            BitSet contrib = (BitSet) sets[i].clone();
            for (int j = 0; j < nNodes; j++) {
                if (j != i) contrib.andNot(sets[j]);
            }
            assert !contrib.isEmpty() : "State had a node contributing no triple";
            contrib.andNot(nodeTriplesBS);
            if (contrib.isEmpty())
                return null; // adding the new node causes an old node to contribute nothing
        }

        // build next state
        long[] next = new long[state.length];
        System.arraycopy(state, 0, next, 0, state.length);
        bs.set(next, NODES, nodeIdx);
        bs.or(next, VARS, nodeVars);
        bs.or(next, TRIPLES, nodeTriples);
        return next;
    }

}
