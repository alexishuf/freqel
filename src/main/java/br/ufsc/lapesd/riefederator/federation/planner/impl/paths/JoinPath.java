package br.ufsc.lapesd.riefederator.federation.planner.impl.paths;

import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.builderWithExpectedSize;

@Immutable
public class JoinPath {
    private final @Nonnull ImmutableList<JoinInfo> joinInfos;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableSet<PlanNode> nodes;
    private @LazyInit int hash = 0;

    public JoinPath(@Nonnull ImmutableList<JoinInfo> joinInfos) {
        checkArgument(!joinInfos.isEmpty());
        this.joinInfos = joinInfos;
        //noinspection UnstableApiUsage
        ImmutableSet.Builder<PlanNode> nodesBuilder = builderWithExpectedSize(joinInfos.size());
        Iterator<JoinInfo> it = joinInfos.iterator();
        JoinInfo current = it.next();
        nodesBuilder.add(current.getLeft());
        nodesBuilder.add(current.getRight());
        for (JoinInfo last = current; it.hasNext(); last = current)
            nodesBuilder.add((current = it.next()).getOppositeToLinked(last));
        nodes = nodesBuilder.build();
    }

    public JoinPath(@Nonnull JoinInfo... infos) {
        this(ImmutableList.copyOf(infos));
    }

    public JoinPath(@Nonnull PlanNode node) {
        joinInfos = ImmutableList.of();
        nodes = ImmutableSet.of(node);
        hash = node.hashCode();
    }

    public @Nonnull ImmutableList<JoinInfo> getJoinInfos() {
        return joinInfos;
    }

    public boolean hasJoins() {
        return !joinInfos.isEmpty();
    }

    public boolean isWhole() {
        return joinInfos.isEmpty();
    }

    public @Nonnull PlanNode getWhole() {
        return nodes.iterator().next();
    }

    public @Nonnull ImmutableSet<PlanNode> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinPath)) return false;
        JoinPath other = (JoinPath) o;
        return hashCode() == other.hashCode() &&
                Objects.equals(nodes, other.nodes);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int[] hashes = new int[nodes.size()];
            int idx = 0;
            for (PlanNode node : nodes) hashes[idx++] = node.hashCode();
            Arrays.sort(hashes);
            HashCodeBuilder builder = new HashCodeBuilder();
            for (int hash : hashes)
                builder.append(hash);
            hash = builder.toHashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        if (isWhole()) {
            return nodes.iterator().next().toString();
        } else {
            assert !joinInfos.isEmpty();
            StringBuilder b = new StringBuilder();
            b.append(joinInfos.iterator().next().getLeft()).append(" -> ");
            for (JoinInfo info : joinInfos) {
                b.append(info.getRight()).append(" -> ");
            }
            b.setLength(b.length()-4);
            return b.toString();
        }
    }
}
