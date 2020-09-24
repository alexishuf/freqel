package br.ufsc.lapesd.riefederator.federation.planner.conjunctive;

import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface JoinGraph {
    @Nonnull RefIndexSet<Op> getNodes();

    default @Nullable JoinInfo getWeight(@Nonnull Op l, @Nonnull Op r) {
        int li = indexOf(l), ri = indexOf(r);
        if (li < 0 || ri < 0)
            throw new IllegalArgumentException("Node not found (indexOf() < 0)");
        return getWeight(li, ri);
    }

    @Nullable JoinInfo getWeight(int l, int r);

    @CanIgnoreReturnValue
    boolean forEachNeighborIndex(int idx, Consumer<Integer> consumer);

    @CanIgnoreReturnValue
    boolean forEachNeighbor(int idx, BiConsumer<JoinInfo, Op> consumer);

    @CanIgnoreReturnValue
    default boolean forEachNeighbor(@Nonnull Op node, BiConsumer<JoinInfo, Op> consumer) {
        int i = indexOf(node);
        if (i < 0)
            throw new IllegalArgumentException("Not not found (indexOf() < 0)");
        return forEachNeighbor(i, consumer);
    }

    default @Nonnull Op get(int idx) {
        return getNodes().get(idx);
    }

    default int indexOf(@Nonnull Op node) {
        return getNodes().indexOf(node);
    }

    default int size() {
        return getNodes().size();
    }

    default boolean isEmpty() {
        return getNodes().isEmpty();
    }
}
