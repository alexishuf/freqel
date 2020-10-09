package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.RawAlignedBitSet;
import br.ufsc.lapesd.riefederator.util.bitset.Bitsets;
import br.ufsc.lapesd.riefederator.util.bitset.SegmentBitset;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InputStateHelper extends AbstractStateHelper {
    public static int INPUTS = 3;
    private final @Nonnull Bitset[] triplesTmp;
    private final @Nonnull Bitset contribTmp;
    private final @Nonnull Bitset nodeOutVars, nodeInVars, stateOutVars;
    private final @Nonnull SegmentBitset stateVarsSegment, stateInVarsSegment;
    private static final long[] DUMMY = new long[1];


    public InputStateHelper(@Nonnull BitJoinGraph graph, @Nonnull IndexSet<String> vars,
                            @Nonnull IndexSubset<Triple> queryTriples) {
        super(graph, vars, queryTriples,
              new RawAlignedBitSet(graph.getNodes().size(), vars.size(),
                                   queryTriples.getParent().size(), vars.size()));
        int nNodes = graph.size();
        triplesTmp = new Bitset[nNodes];
        contribTmp = Bitsets.createFixed(nNodes);
        int nVars = vars.size();
        nodeOutVars = Bitsets.createFixed(nVars);
        nodeInVars = Bitsets.createFixed(nVars);
        stateOutVars = Bitsets.createFixed(nVars);
        stateVarsSegment = new SegmentBitset(DUMMY, 0, 1);
        stateInVarsSegment = new SegmentBitset(DUMMY, 0, 1);
    }

    @Override public boolean isFinal(@Nonnull long[] state) {
        return super.isFinal(state) && bs.isEmpty(state, INPUTS);
    }

    public @Nonnull long[] createState(int nodeIdx) {
        long[] data = bs.alloc();
        bs.set(data, NODES, nodeIdx);
        Op node = nodes.get(nodeIdx);
        bs.or(data, VARS, ((IndexSubset<String>)node.getPublicVars()).getBitset());
        bs.or(data, TRIPLES, ((IndexSubset<Triple>)node.getMatchedTriples()).getBitset());
        bs.or(data, INPUTS, ((IndexSubset<String>)node.getRequiredInputVars()).getBitset());
        return data;
    }

    public @Nullable long[] addNode(@Nonnull long[] state, int nodeIdx) {
        if (bs.get(state, NODES, nodeIdx))
            return null;
        Op n = graph.get(nodeIdx);
        assert ((IndexSubset<String>)n.getResultVars()).getParent() == vars;
        assert ((IndexSubset<Triple>)n.getMatchedTriples()).getParent() == triples;

        Bitset nodeVars = ((IndexSubset<String>) n.getResultVars()).getBitset();
        assert nodeVars.intersects(bs.asBitset(state, VARS)) : "New node shares no var with state";

        // check triples subsumption
        Bitset nodeTriples = ((IndexSubset<Triple>) n.getMatchedTriples()).getBitset();
        if (bs.containsAll(nodeTriples, state, TRIPLES)
                || bs.containsAll(state, TRIPLES, nodeTriples)) {
            return null; //one triple set subsumes the other
        }

        // check if any old node would be subsumed by the other nodes and the new node
        int nNodes = 0;
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i+1))
            triplesTmp[nNodes++] = ((IndexSubset<Triple>) graph.get(i).getMatchedTriples()).getBitset();
        for (int i = 0; i < nNodes; i++) {
            contribTmp.assign(triplesTmp[i]);
            for (int j = 0; j < nNodes; j++) {
                if (i != j) contribTmp.andNot(triplesTmp[j]);
            }
            assert !contribTmp.isEmpty() : "State had node that contributed with  no triple!";
            contribTmp.andNot(nodeTriples);
            if (contribTmp.isEmpty())
                return null; // adding this node causes an old node to be subsumed
        }

        // handle node inputs/output vars ...
        nodeInVars.assign(((IndexSubset<String>)n.getRequiredInputVars()).getBitset());
        nodeOutVars.assign(((IndexSubset<String>)n.getPublicVars()).getBitset());
        nodeOutVars.andNot(nodeInVars);

        // non-input vars provided by state
        stateVarsSegment  .map(state, bs.componentBegin(VARS),   bs.componentEnd(VARS)  );
        stateInVarsSegment.map(state, bs.componentBegin(INPUTS), bs.componentEnd(INPUTS));
        stateOutVars.assign(stateVarsSegment);
        stateOutVars.andNot(stateInVarsSegment);

        // vars that are provided by the state are not considered inputs anymore
        nodeInVars.andNot(stateOutVars);

        // fill new state array
        long[] next = bs.alloc();
        System.arraycopy(state, 0, next, 0, state.length);
        bs   .set(next, NODES,   nodeIdx    );
        bs    .or(next, VARS,    nodeVars   );
        bs    .or(next, TRIPLES, nodeTriples);
        bs.andNot(next, INPUTS,  nodeOutVars); // remove inputs satisfied by node
        bs    .or(next, INPUTS,  nodeInVars ); // add new input vars
        return next;
    }
}
