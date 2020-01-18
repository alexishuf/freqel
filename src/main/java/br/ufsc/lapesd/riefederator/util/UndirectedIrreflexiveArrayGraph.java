package br.ufsc.lapesd.riefederator.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static java.lang.System.arraycopy;

public abstract class UndirectedIrreflexiveArrayGraph<N> {
    private final @Nonnull List<N> nodes;
    private float[] weights;
    private int age = 0;

    @VisibleForTesting
    static int totalCells(int nodesCount) {
        return nodesCount == 0 ? 0 : ((nodesCount-1)*nodesCount)/2;
    }

    @VisibleForTesting
    int rowOffset(int nodeIdx) {
        int size = nodes.size();
        return totalCells(size) - totalCells(size - nodeIdx);
    }

    public UndirectedIrreflexiveArrayGraph(@Nonnull List<N> nodes) {
        Preconditions.checkArgument(!nodes.isEmpty());
        Preconditions.checkArgument(nodes.stream().noneMatch(Objects::isNull),
                "There can be no null node");
        this.nodes = nodes;
        int size = nodes.size();
        weights = new float[totalCells(size)];
        int idx = 0;
        for (int i = 0; i < size-1; i++) {
            for (int j = i+1; j < size; j++)
                weights[idx++] = weigh(nodes.get(i), nodes.get(j));
        }
    }

    protected abstract float weigh(@Nonnull N l, @Nonnull N r);

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

    public float getWeight(int l, int r) {
        Preconditions.checkArgument(l != r, "Cannot getWeight("+l+", "+r+") on irreflexive graph");
        if (l > r) return getWeight(r, l);
        return weights[rowOffset(l) + (r-(l+1))];
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
    public float[] getWeights() {
        int cells = totalCells(size());
        float[] copy = new float[cells];
        arraycopy(weights, 0, copy, 0, cells);
        return copy;
    }

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append("UndirectedIrreflexiveArrayGraph{\n");
        int size = nodes.size();
        for (int i = 0; i < size; i++) {
            b.append("  [");
            for (int j =   0; j <= i  ; j++) b.append(" 0.00, ");
            for (int j = i+1; j < size; j++) b.append(String.format("%5.2f, ", getWeight(i, j)));
            b.setLength(b.length()-2);
            b.append("] ").append(nodes.get(i)).append("\n");
        }
        b.append("}");
        return b.toString();
    }
}
