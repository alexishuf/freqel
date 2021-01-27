package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.RawAlignedBitSet;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class NoInputStateHelper extends AbstractStateHelper {
    private final @Nonnull Bitset[] triplesTmp;
    private final @Nonnull Bitset[] contribParts;
    private final @Nonnull Bitset contribTmp;

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

        Bitset nodeTriples = matchedTriples[nodeIdx];
        if (bs.containsAll(nodeTriples, state, TRIPLES)
                || bs.containsAll(state, TRIPLES, nodeTriples)) {
            return null; //one triple set subsumes the other
        }

        // check if any old node is subsumed by all other old nodes and the new node
        contribParts[0].assign(nodeTriples);
        contribParts[1].assign(nodeTriples);
        int nNodes = 0;
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i+1)) {
            Bitset bs = matchedTriples[i];
            contribParts[nNodes & 0x1].or(bs);
            triplesTmp[nNodes++] = bs;
        }
        switch (nNodes) {
            case 0: break;
            case 1:
            case 2:
                if (contribParts[1].containsAll(triplesTmp[0])) return null;
                if (nNodes == 2 && contribParts[0].containsAll(triplesTmp[1])) return null;
                break;
            default:
                for (int i = 0; i < nNodes; i++) {
                    contribTmp.assign(triplesTmp[i]);
                    int part = i & 0x1;
                    contribTmp.andNot(contribParts[part ^ 0x1]);
                    for (int j = part; j < i     ; j += 2) contribTmp.andNot(triplesTmp[j]);
                    for (int j = i+2 ; j < nNodes; j += 2) contribTmp.andNot(triplesTmp[j]);
                    if (contribTmp.isEmpty())
                        return null; // adding the new node causes an old node to contribute nothing
                }
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
