package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.RawAlignedBitSet;
import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class NoInputStateHelper extends AbstractStateHelper {
    private @Nonnull Bitset[] triplesTmp;
    private @Nonnull Bitset[] contribParts;
    private @Nonnull Bitset contribTmp;

    public NoInputStateHelper(@Nonnull BitJoinGraph graph, @Nonnull IndexSet<String> vars,
                              @Nonnull IndexSubset<Triple> queryTriples) {
        super(graph, vars, queryTriples,
              new RawAlignedBitSet(graph.getNodes().size(), vars.size(),
                                   queryTriples.getParent().size()));
        int nNodes = graph.size();
        triplesTmp = new Bitset[nNodes];
        contribTmp = Bitsets.createFixed(nNodes);
        contribParts = new Bitset[] {Bitsets.createFixed(nNodes), Bitsets.createFixed(nNodes)};
    }

    public @Nonnull long[] createState(int nodeIdx) {
        long[] data = bs.alloc();
        bs.set(data, NODES, nodeIdx);
        Op node = nodes.get(nodeIdx);
        bs.or(data, VARS, ((IndexSubset<String>)node.getPublicVars()).getBitset());
        bs.or(data, TRIPLES, ((IndexSubset<Triple>)node.getMatchedTriples()).getBitset());
        return data;
    }

    public @Nullable long[] addNode(@Nonnull long[] state, int nodeIdx) {
        if (bs.get(state, NODES, nodeIdx))
            return null; // node already in state
        Op n = graph.get(nodeIdx);
        assert ((IndexSubset<String>)n.getResultVars()).getParent() == vars;
        assert ((IndexSubset<Triple>) n.getMatchedTriples()).getParent() == triples;

        Bitset nodeVars = ((IndexSubset<String>) n.getResultVars()).getBitset();
        assert nodeVars.intersects(bs.asBitset(state, VARS)) : "New node shares no variable";

        Bitset nodeTriples = ((IndexSubset<Triple>) n.getMatchedTriples()).getBitset();
        if (bs.containsAll(nodeTriples, state, TRIPLES)
                || bs.containsAll(state, TRIPLES, nodeTriples)) {
            return null; //one triple set subsumes the other
        }

        // check if any old node is subsumed by all other old nodes and the new node
        contribParts[0].assign(nodeTriples);
        contribParts[1].assign(nodeTriples);
        int nNodes = 0;
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i+1)) {
            Bitset bs = ((IndexSubset<Triple>) graph.get(i).getMatchedTriples()).getBitset();
            contribParts[nNodes & 0x1].or(bs);
            triplesTmp[nNodes++] = bs;
        }
        for (int i = 0; i < nNodes; i++) {
            contribTmp.assign(triplesTmp[i]);
            int part = i & 0x1;
            contribTmp.andNot(contribParts[part ^ 0x1]);
            for (int j = part; j < i     ; j += 2) contribTmp.andNot(triplesTmp[j]);
            for (int j = i+2 ; j < nNodes; j += 2) contribTmp.andNot(triplesTmp[j]);
            if (contribTmp.isEmpty())
                return null; // adding the new node causes an old node to contribute nothing
        }

        // build next state
        long[] next = bs.alloc();
        System.arraycopy(state, 0, next, 0, state.length);
        bs.set(next, NODES, nodeIdx);
        bs.or(next, VARS, nodeVars);
        bs.or(next, TRIPLES, nodeTriples);
        return next;
    }

}
