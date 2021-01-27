package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv;

import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.JoinGraph;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.util.RawAlignedBitSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static br.ufsc.lapesd.freqel.util.RawAlignedBitSet.nextSet;

public class BitJoinGraph implements JoinGraph {
    protected final RefIndexSet<Op> nodes;
    protected  @Nonnull long[] adj;
    protected  @Nonnull JoinInfo[] infos;
    protected int nNodes, adjBits;

    public BitJoinGraph(@Nonnull RefIndexSet<Op> nodes) {
        this.nodes   = nodes;
        this.nNodes  = nodes.size();
        this.adjBits = adjBits(nNodes);
        this.adj     = new long[RawAlignedBitSet.wordsRequired(adjBits)];
        this.infos   = new JoinInfo[adjBits];
        fillAdjacencyMatrix(0);
    }

    static int adjBits(int nodes) {
        return (nodes * (nodes-1)) / 2;
    }

    @VisibleForTesting
    int adjBit(int l) {
        return adjBit(l, l+1);
    }

    @VisibleForTesting
    int adjBit(int l, int r) {
        return adjBit(l, r, adjBits, nNodes);
    }

    @VisibleForTesting
    static int adjBit(int l, int r, int adjBits, int nNodes) {
        assert l < r;
        return adjBits - adjBits(nNodes-l) + (r-l-1);
    }

    protected static class AdjacencyChecker {
        Op left, right;
        IndexSubset<String> lv, rv;
        IndexSubset<Triple> lt, rt;

        public void setLeft(@Nonnull Op left) {
            this.left = left;
            lv = (IndexSubset<String>)left.getPublicVars();
            lt = (IndexSubset<Triple>)left.getMatchedTriples();
        }

        public void setRight(@Nonnull Op right) {
            this.right = right;
            rv = (IndexSubset<String>)right.getPublicVars();
            rt = (IndexSubset<Triple>)right.getMatchedTriples();
        }

        public boolean checkAdjacent() {
            assert !left.hasInputs() && !right.hasInputs() : "Inputs are not supported";
            return lv.containsAny(rv) && !lt.containsAll(rt) && !rt.containsAll(lt);
        }

        public @Nonnull JoinInfo getJoinInfo() {
            assert checkAdjacent();
            return new JoinInfo(left, right, lv.createIntersection(rv));
        }
    }

    protected @Nonnull AdjacencyChecker createAdjacencyChecker() {
        return new AdjacencyChecker();
    }


    @VisibleForTesting
    void fillAdjacencyMatrix(int startNodeIdx) {
        int nodesCount = nodes.size();
        assert startNodeIdx < nodesCount || nodesCount == 0;
        AdjacencyChecker checker = createAdjacencyChecker();
        for (int i = startNodeIdx; i < nodesCount-1; i++) {
            checker.setLeft(nodes.get(i));
            for (int j = i+1; j < nodesCount; j++) {
                checker.setRight(nodes.get(j));
                if (checker.checkAdjacent()) {
                    int bitIndex = adjBit(i, j);
                    RawAlignedBitSet.set(adj, bitIndex);
                    infos[bitIndex] = checker.getJoinInfo();
                }
            }
        }
    }

    void backwardFillAdjacencyMatrix(int startNodeIdx) {
        int nodesCount = nodes.size();
        assert startNodeIdx < nodesCount || nodesCount == 0;
        AdjacencyChecker checker = createAdjacencyChecker();
        for (int i = 0; i < nodesCount-1; i++) {
            checker.setLeft(nodes.get(i));
            for (int j = startNodeIdx; j < nodesCount; j++) {
                checker.setRight(nodes.get(j));
                if (checker.checkAdjacent()) {
                    int bitIndex = adjBit(i, j);
                    RawAlignedBitSet.set(adj, bitIndex);
                    infos[bitIndex] = checker.getJoinInfo();
                }
            }
        }
    }

    @Override public @Nonnull RefIndexSet<Op> getNodes() {
        return nodes;
    }

    @Override public @Nullable JoinInfo getWeight(int l, int r) {
        if (l == r)
            return null;
        return infos[adjBit(Math.min(l, r), Math.max(l, r))];
    }

    @Override public boolean forEachNeighborIndex(int idx, Consumer<Integer> consumer) {
        boolean has = false;
        for (int i = 0; i < idx; i++) {
            if (RawAlignedBitSet.get(adj, adjBit(i, idx))) {
                has = true;
                consumer.accept(i);
            }
        }
        int begin = adjBit(idx), end = adjBit(idx+1);
        int adjustment = -begin + idx + 1; // begin bit represents node idx+1
        for (int i = nextSet(adj, begin); i >= 0 && i < end; i = nextSet(adj, i+1)) {
            has = true;
            consumer.accept(i + adjustment);
        }
        return has;
    }

    @Override public boolean forEachNeighbor(int idx, BiConsumer<JoinInfo, Op> consumer) {
        boolean has = false;
        for (int i = 0; i < idx; i++) {
            JoinInfo info = infos[adjBit(i, idx)];
            if (info != null) {
                has = true;
                consumer.accept(info, info.getLeft());
            }
        }
        for (int i = adjBit(idx), end = adjBit(idx+1); i < end; i++) {
            JoinInfo info = infos[i];
            if (info != null) {
                has = true;
                consumer.accept(info, info.getRight());
            }
        }
        return has;
    }

    @Override public @Nonnull Op get(int idx) {
        return nodes.get(idx);
    }

    @Override public int indexOf(@Nonnull Op node) {
        return nodes.indexOf(node);
    }

    @Override public int size() {
        return nNodes;
    }

    @Override public boolean isEmpty() {
        return nodes.isEmpty();
    }

    public void notifyAddedNodes() {
        int nNodes2 = nodes.size(), nNodesOld = nNodes;
        if (nNodes2 == nNodes)
            return; //no work
        int adjBits2      = adjBits(nNodes2);
        long[] adj2       = new long[RawAlignedBitSet.wordsRequired(adjBits2)];
        JoinInfo[] infos2 = new JoinInfo[adjBits2];

        // copy for old nodes
        for (int i = 0; i < nNodes - 1; i++) {
            for (int j = i+1; j < nNodes; j++) {
                int bitIdx  = adjBit(i, j, adjBits, nNodes);
                int bitIdx2 = adjBit(i, j, adjBits2, nNodes2);
                if (RawAlignedBitSet.get(adj, bitIdx))
                    RawAlignedBitSet.set(adj2, bitIdx2);
                infos2[bitIdx2] = infos[bitIdx];
            }
        }
        // update fields
        this.adjBits = adjBits2;
        this.adj     = adj2;
        this.infos   = infos2;
        this.nNodes  = nNodes2;
        // compute among new nodes
        fillAdjacencyMatrix(nNodesOld);
        // compute for old nodes <--> new nodes
        backwardFillAdjacencyMatrix(nNodesOld);
    }
}
