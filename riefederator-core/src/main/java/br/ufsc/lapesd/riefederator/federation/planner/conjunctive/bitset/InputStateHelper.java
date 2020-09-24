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

public class InputStateHelper extends AbstractStateHelper {
    public static int INPUTS = 3;

    public InputStateHelper(@Nonnull BitJoinGraph graph, @Nonnull IndexSet<String> vars,
                            @Nonnull IndexSubset<Triple> queryTriples) {
        super(graph, vars, queryTriples,
              new RawAlignedBitSet(graph.getNodes().size(), vars.size(),
                                   queryTriples.getParent().size(), vars.size()));
    }

    @Override public boolean isFinal(@Nonnull long[] state) {
        return super.isFinal(state) && bs.isEmpty(state, INPUTS);
    }

    public @Nonnull long[] createState(int nodeIdx) {
        long[] data = bs.alloc(), tmp;
        bs.set(data, NODES, nodeIdx);
        Op node = nodes.get(nodeIdx);
        tmp = ((IndexSubset<String>)node.getPublicVars()).getBitSet().toLongArray();
        bs.or(data, VARS, tmp);
        tmp = ((IndexSubset<Triple>)node.getMatchedTriples()).getBitSet().toLongArray();
        bs.or(data, TRIPLES, tmp);
        tmp = ((IndexSubset<String>)node.getRequiredInputVars()).getBitSet().toLongArray();
        bs.or(data, INPUTS, tmp);
        return data;
    }

    public @Nullable long[] addNode(@Nonnull long[] state, int nodeIdx) {
        if (bs.get(state, NODES, nodeIdx))
            return null;
        Op n = graph.get(nodeIdx);
        assert ((IndexSubset<String>)n.getResultVars()).getParent() == vars;
        assert ((IndexSubset<Triple>)n.getMatchedTriples()).getParent() == triples;

        long[] nodeVars = ((IndexSubset<String>) n.getResultVars()).getBitSet().toLongArray();
        assert bs.intersects(nodeVars, state, VARS) : "New node shares no var with state";

        // check triples subsumption
        BitSet nodeTriplesBS = ((IndexSubset<Triple>) n.getMatchedTriples()).getBitSet();
        long[] nodeTriples = nodeTriplesBS.toLongArray();
        if (bs.containsAll(nodeTriples, state, TRIPLES)
                || bs.containsAll(state, TRIPLES, nodeTriples)) {
            return null; //one triple set subsumes the other
        }

        // check if any old node would be subsumed by the other nodes and the new node
        int nNodes = 0;
        BitSet[] sets = new BitSet[graph.size()];
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i+1))
            sets[nNodes++] = ((IndexSubset<Triple>) graph.get(i).getMatchedTriples()).getBitSet();
        for (int i = 0; i < nNodes; i++) {
            BitSet contrib = (BitSet) sets[i].clone();
            for (int j = 0; j < nNodes; j++) {
                if (i != j) contrib.andNot(sets[j]);
            }
            assert !contrib.isEmpty() : "State had node that contributed with  no triple!";
            contrib.andNot(nodeTriplesBS);
            if (contrib.isEmpty())
                return null; // adding this node causes an old node to be subsumed
        }

        // handle inputs vars ...
        long[] nodeInVars = ((IndexSubset<String>)n.getRequiredInputVars())
                                                   .getBitSet().toLongArray();
        // non-input vars provided by node
        long[] nodeOutVars = new long[nodeVars.length];
        RawAlignedBitSet.andNot(nodeOutVars, nodeVars, nodeInVars);

        // non-input vars provided by state
        long[] stateOutVars = new long[bs.componentWords(VARS)];
        bs.andNot(stateOutVars, state, VARS, state, INPUTS);

        // vars that are provided by the state are not considered inputs anymore
        RawAlignedBitSet.andNot(nodeInVars, stateOutVars);

        // fill new state array
        long[] next = new long[state.length];
        System.arraycopy(state, 0, next, 0, state.length);
        bs.set(   next, NODES,   nodeIdx    );
        bs.or(    next, VARS,    nodeVars   );
        bs.or(    next, TRIPLES, nodeTriples);
        bs.andNot(next, INPUTS,  nodeOutVars); // remove inputs satisfied by node
        bs.or(    next, INPUTS,  nodeInVars);  // add new input vars
        return next;
    }
}
