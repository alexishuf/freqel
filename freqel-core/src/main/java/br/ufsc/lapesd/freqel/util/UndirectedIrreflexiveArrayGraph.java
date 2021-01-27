package br.ufsc.lapesd.freqel.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.lang.System.arraycopy;

@SuppressWarnings("unchecked")
public abstract class UndirectedIrreflexiveArrayGraph<N, W> {
    private @Nonnull List<N> nodes;
    private W[] weights;
    private int age = 0;
    private final W zero;
    protected final @Nonnull Class<W> wClass;

    @VisibleForTesting
    static int totalCells(int nodesCount) {
        return nodesCount == 0 ? 0 : ((nodesCount-1)*nodesCount)/2;
    }

    @VisibleForTesting
    int rowOffset(int nodeIdx) {
        int size = nodes.size();
        return totalCells(size) - totalCells(size - nodeIdx);
    }

    protected UndirectedIrreflexiveArrayGraph(@Nonnull Class<W> wClass, @Nonnull List<N> nodes) {
        this(wClass, null, nodes);
    }

    protected UndirectedIrreflexiveArrayGraph(@Nonnull Class<W> wClass, @Nullable W zero,
                                           @Nonnull List<N> nodes) {
        Preconditions.checkArgument(!nodes.isEmpty());
        assert nodes.stream().noneMatch(Objects::isNull) : "There can be no null node";
        this.nodes = nodes;
        this.wClass = wClass;
        this.zero = zero;
        int size = nodes.size();
        weights = (W[])Array.newInstance(wClass, totalCells(size));
        int idx = 0;
        for (int i = 0; i < size-1; i++) {
            for (int j = i+1; j < size; j++)
                weights[idx++] = weigh(nodes.get(i), nodes.get(j));
        }
    }

    protected UndirectedIrreflexiveArrayGraph(@Nonnull Class<W> wClass, @Nullable W zero) {
        nodes = Collections.emptyList();
        this.wClass = wClass;
        this.zero = zero;
        weights = (W[])Array.newInstance(wClass, 0);
    }

    protected UndirectedIrreflexiveArrayGraph(@Nonnull Class<W> wClass) {
        this(wClass, (W)null);
    }

    protected UndirectedIrreflexiveArrayGraph(@Nonnull Class<W> wClass, @Nullable W zero,
                                              @Nonnull List<N> nodes, @Nonnull W[] weights) {
        Preconditions.checkArgument(weights.length >= totalCells(nodes.size()));
        assert nodes.stream().noneMatch(Objects::isNull) : "Nodes cannot be null";
        this.nodes = nodes;
        this.wClass = wClass;
        this.zero = zero;
        this.weights = weights;
    }

    public void stealFrom(@Nonnull UndirectedIrreflexiveArrayGraph<N, W> other) {
        this.weights = other.weights;
        this.nodes = other.nodes;
        assert Objects.equals(zero, other.zero);
        other.weights = (W[])Array.newInstance(other.wClass, 0);
        other.nodes = Collections.emptyList();
    }

    protected abstract @Nullable W weigh(@Nonnull N l, @Nonnull N r);

    public void remove(@Nonnull N node) {
        int idx = nodes.indexOf(node);
        if (idx < 0) throw new NoSuchElementException(node+" is not a node of the graph");
        removeAt(idx);
    }

    public void removeAt(int idx) {
        int startAge = age;
        int size = nodes.size();
        Preconditions.checkPositionIndex(idx, size);

        // Remove references to idx in nodes before idx
        // Removal of idx=2 in 4-node graph [ 1 2 3   1 2   1]
        // i=0, out=0, columns=3, cellsBefore=1, cellsAfter=1, rowOffset=0
        //  [ 1 2 3   1 2   1]
        //  [ 1 3 _   1 2   1]
        // i=1 out=2, columns=2, cellsBefore=0, cellsAfter=1, rowOffset=3
        //  [ 1 3 _   1 2   1]
        //  [ 1 3 2   _ _   1]
        int out = 0;
        for (int columns = size-1, i = 0; i < idx; i++, --columns) {
            int cellsBefore = idx-1-i, cellsAfter = columns-(idx-i);
            int rowOffset = rowOffset(i);
            arraycopy(weights, rowOffset, weights, out, cellsBefore);
            out += cellsBefore;
            arraycopy(weights, rowOffset+cellsBefore+1, weights, out, cellsAfter);
            out += cellsAfter;
        }

        // Shift the nodes after idx down to the next free cell
        int cellsAfterIdx = totalCells(size - (idx + 1));
        arraycopy(weights, rowOffset(idx+1), weights, out, cellsAfterIdx);

        // Remove the node itself
        nodes.remove(idx);

        if (++age != startAge+1) //fail-fast on concurrency violations
            throw new ConcurrentModificationException();
    }

    public void replaceFirst(@Nonnull N node, @Nonnull N replacement) {
        if (node == replacement) return;
        int idx = nodes.indexOf(node);
        if (idx < 0) throw new NoSuchElementException("No node "+node+" in this graph");
        replaceAt(idx, replacement);
    }

    public void replaceAt(int idx, @Nonnull N replacement) {
        Preconditions.checkPositionIndex(idx, nodes.size());
        if (nodes.get(idx) == replacement) return;
        int startAge = this.age;
        nodes.set(idx, replacement);

        // update weights for nodes before idx
        for (int i = 0; i < idx; i++)
            weights[rowOffset(i) + (idx-(i+1))] = weigh(nodes.get(i), replacement);

        // update weights for nodes after idx
        int size = nodes.size(), rowStart = rowOffset(idx);
        for (int i = 0; idx+1+i < size; i++)
            weights[rowStart+i] = weigh(replacement, nodes.get(idx+1+i));

        if (++age != startAge+1) //fail-fast on concurrency violations
            throw new ConcurrentModificationException();
    }

    public W getWeight(@Nonnull N l, @Nonnull N r) {
        return getWeight(indexOf(l), indexOf(r));
    }

    public W getWeight(int l, int r) {
        Preconditions.checkPositionIndex(l, size());
        Preconditions.checkPositionIndex(r, size());
        if (l == r)
            return null;
        if (l > r) return getWeight(r, l);
        return weights[rowOffset(l) + (r-(l+1))];
    }

    @CanIgnoreReturnValue
    public boolean forEachNeighborIndex(int idx, @Nonnull Consumer<Integer> consumer) {
        Preconditions.checkPositionIndex(idx, size());
        boolean got = false;
        for (int i = 0; i < idx; i++) {
            W w = weights[rowOffset(i) + (idx - (i + 1))];
            if (!Objects.equals(w, zero)) {
                consumer.accept(i);
                got = true;
            }
        }
        int rowOffset = rowOffset(idx);
        for (int i = idx+1; i < size(); i++) {
            W w = weights[rowOffset + (i - (idx + 1))];
            if (!Objects.equals(w, zero)) {
                consumer.accept(i);
                got = true;
            }
        }
        return got;
    }

    @CanIgnoreReturnValue
    public boolean forEachNeighbor(int idx, BiConsumer<W, N> consumer) {
        Preconditions.checkPositionIndex(idx, size());
        boolean got = false;
        for (int i = 0; i < idx; i++) {
            W w = weights[rowOffset(i) + (idx - (i + 1))];
            if (!Objects.equals(w, zero)) {
                consumer.accept(w, get(i));
                got = true;
            }
        }
        int rowOffset = rowOffset(idx);
        for (int i = idx+1; i < size(); i++) {
            W w = weights[rowOffset + (i - (idx + 1))];
            if (!Objects.equals(w, zero)) {
                consumer.accept(w, get(i));
                got = true;
            }
        }
        return got;
    }

    @CanIgnoreReturnValue
    public boolean forEachNeighbor(@Nonnull N node, @Nonnull BiConsumer<W, N> consumer) {
        return forEachNeighbor(indexOf(node), consumer);
    }

    public @Nonnull List<N> getNodes() {
        return nodes;
    }
    public @Nonnull N get(int idx) {
        Preconditions.checkPositionIndex(idx, size());
        return nodes.get(idx);
    }
    public int indexOf(@Nonnull N node) {
        return nodes.indexOf(node);
    }
    public int size() {
        return nodes.size();
    }
    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    /**
     * This should be used only for testing purposes.
     *
     * @return A <b>copy</b> of the weights array trimmed according to size()
     */
    @VisibleForTesting
    public W[] getWeights() {
        int cells = totalCells(size());
        W[] copy = (W[])Array.newInstance(wClass, cells);
        arraycopy(weights, 0, copy, 0, cells);
        return copy;
    }

    /**
     * Gets a copy of the float[] weights array for constructing a subset Graph with
     * the given nodes subset.
     *
     * @param subset {@link BitSet} with 1 at the position of the nodes to include
     * @param outNodes If non-null will receive (though add() calls) the nodes specified by subset
     */
    protected @Nonnull W[]
    getWeightsForSubset(@Nonnull BitSet subset, @Nullable List<N> outNodes) {
        Preconditions.checkArgument(subset.length() <= size(), "subset has a bit  set >= size()");
        Preconditions.checkArgument(outNodes == null || outNodes.isEmpty(),
                "Non-empty outNodes. It will not correspond to the returned weights array!");
        int[] idxs = new int[size()];
        int subsetSize = 0;
        for (int i = subset.nextSetBit(0); i >= 0; i = subset.nextSetBit(i+1)) {
            if (outNodes != null)
                outNodes.add(nodes.get(i));
            idxs[subsetSize++] = i;
        }

        W[] subWeights = (W[])Array.newInstance(wClass, totalCells(subsetSize));
        int out = 0;
        for (int i = 0; i < subsetSize; i++) {
            for (int j = i+1; j < subsetSize; j++)
                subWeights[out++] = getWeight(idxs[i], idxs[j]);
        }
        assert out == subWeights.length;

        return subWeights;
    }

    protected @Nonnull String toString(@Nullable W w) {
        if (w == null) {
            return zeroString();
        } else if (w instanceof Float || w instanceof Double) {
            double d = (double)w;
            return String.format("%5.2f", d);
        } else if (w instanceof Number) {
            return String.format("%3d", ((Number) w).longValue());
        }
        return w.toString();
    }

    protected @Nonnull String zeroString() {
        if (wClass.isAssignableFrom(Float.class) || wClass.isAssignableFrom(Double.class))
            return "     "; //5 spaces
        else if (Number.class.isAssignableFrom(wClass))
            return "   "; //3 spaces
        else
            return String.valueOf(zero);
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append("UndirectedIrreflexiveArrayGraph{\n");
        int size = nodes.size();
        String zero = zeroString();
        for (int i = 0; i < size; i++) {
            b.append("  [");
            for (int j =   0; j <= i  ; j++) b.append(zero).append("  ");
            for (int j = i+1; j < size; j++)
                b.append(toString(getWeight(i, j))).append(", ");
            b.setLength(b.length()-2);
            b.append("] ").append(nodes.get(i)).append("\n");
        }
        b.append("}");
        return b.toString();
    }
}
