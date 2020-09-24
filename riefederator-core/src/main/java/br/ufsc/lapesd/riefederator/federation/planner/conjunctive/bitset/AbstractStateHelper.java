package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.RawAlignedBitSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

public abstract class AbstractStateHelper {
    final @Nonnull RefIndexSet<Op> nodes;
    final @Nonnull IndexSet<String> vars;
    final @Nonnull IndexSet<Triple> triples;
    final @Nonnull long[] queryTriples;
    final @Nonnull BitJoinGraph graph;
    final @Nonnull RawAlignedBitSet bs;

    public static final int NODES   = 0;
    public static final int VARS    = 1;
    public static final int TRIPLES = 2;

    protected AbstractStateHelper(@Nonnull BitJoinGraph graph, @Nonnull IndexSet<String> vars,
                               @Nonnull IndexSubset<Triple> queryTriples,
                               @Nonnull RawAlignedBitSet bs) {
        this.nodes = graph.getNodes();
        this.vars = vars;
        this.triples = queryTriples.getParent();
        this.queryTriples = queryTriples.getBitSet().toLongArray();
        this.graph = graph;
        this.bs = bs;
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

    public void forEachNeighbor(@Nonnull long[] state, @Nonnull Consumer<Integer> f) {
        long[] visited = new long[graph.size()];
        int i = bs.nextSet(state, NODES, 0);
        for (; i >= 0; i = bs.nextSet(state, NODES, i + 1)) {
            graph.forEachNeighborIndex(i, j -> {
                if (!RawAlignedBitSet.get(visited, j)) {
                    RawAlignedBitSet.set(visited, j);
                    f.accept(j);
                }
            });
        }
    }
}
