package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.RawAlignedBitSet;
import br.ufsc.lapesd.freqel.util.bitset.Bitsets;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.function.Consumer;

public abstract class AbstractStateHelper {
    final @Nonnull RefIndexSet<Op> nodes;
    final @Nonnull IndexSet<String> vars;
    final @Nonnull IndexSet<Triple> triples;
    final @Nonnull Bitset queryTriples;
    final @Nonnull BitJoinGraph graph;
    final @Nonnull RawAlignedBitSet bs;
    final @Nonnull Bitset forEachNeighborVisited;
    final @Nonnull Bitset[] matchedTriples;

    public static final int NODES   = 0;
    public static final int VARS    = 1;
    public static final int TRIPLES = 2;

    protected AbstractStateHelper(@Nonnull BitJoinGraph graph, @Nonnull IndexSet<String> vars,
                                  @Nonnull IndexSubset<Triple> queryTriples,
                                  @Nonnull RawAlignedBitSet bs) {
        this.nodes = graph.getNodes();
        this.vars = vars;
        this.triples = queryTriples.getParent();
        this.queryTriples = queryTriples.getBitset();
        this.graph = graph;
        this.bs = bs;
        int nNodes = graph.size();
        this.forEachNeighborVisited = Bitsets.createFixed(nNodes);
        this.matchedTriples = new Bitset[nNodes];
        for (int i = 0; i < nNodes; i++)
            matchedTriples[i] = ((IndexSubset<Triple>) graph.get(i).getMatchedTriples()).getBitset();
    }


    public @Nonnull long[] createState() {
        return bs.alloc();
    }

    public abstract @Nonnull long[] createState(int nodeIdx);

    public abstract @Nullable long[] addNode(@Nonnull long[] state, int nodeIdx);

    public boolean isFinal(@Nonnull long[] state) {
        assert bs.containsAll(queryTriples, state, TRIPLES) : "Unexpected triples in state";
        return bs.equals(state, TRIPLES, queryTriples);
    }

    public void forEachNeighbor(@Nonnull long[] state, @Nonnull ArrayDeque<long[]> stack) {
        forEachNeighborVisited.clear();
        Consumer<Integer> consumer = i -> {
            if (forEachNeighborVisited.compareAndSet(i)) {
                long[] next = addNode(state, i);
                if (next != null)
                    stack.push(next);
            }
        };
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i + 1))
            graph.forEachNeighborIndex(i, consumer);
    }

    public void forEachNeighbor(@Nonnull long[] state, @Nonnull Consumer<Integer> f) {
        forEachNeighborVisited.clear();
        Consumer<Integer> consumer = i -> {
            if (forEachNeighborVisited.compareAndSet(i))
                f.accept(i);
        };
        for (int i = bs.nextSet(state, NODES, 0); i >= 0; i = bs.nextSet(state, NODES, i + 1))
            graph.forEachNeighborIndex(i, consumer);
    }
}
