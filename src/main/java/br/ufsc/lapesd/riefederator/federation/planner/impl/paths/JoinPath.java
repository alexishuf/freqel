package br.ufsc.lapesd.riefederator.federation.planner.impl.paths;

import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.util.ImmutableIndexedSubset;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toSet;

@Immutable
public class JoinPath {
    private final @Nonnull ImmutableList<JoinInfo> joinInfos;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableIndexedSubset<PlanNode> nodes;
    private @LazyInit int hash = 0;

    public JoinPath(@Nonnull IndexedSet<PlanNode> allNodes,
                    @Nonnull ImmutableList<JoinInfo> joinInfos) {
        checkArgument(!joinInfos.isEmpty());
        if (JoinPath.class.desiredAssertionStatus()) {
            checkArgument(joinInfos.stream().allMatch(j -> allNodes.containsAll(j.getNodes())),
                          "There are operands of JoinInfos ∉ allNodes");
        }

        this.joinInfos = joinInfos;
        IndexedSubset<PlanNode> nodes = allNodes.emptySubset();
        Iterator<JoinInfo> it = joinInfos.iterator();
        JoinInfo current = it.next();
        nodes.add(current.getLeft());
        nodes.add(current.getRight());
        for (JoinInfo last = current; it.hasNext(); last = current)
            nodes.add((current = it.next()).getOppositeToLinked(last));
        this.nodes = ImmutableIndexedSubset.copyOf(nodes);

        if (JoinPath.class.desiredAssertionStatus()) {
            Set<PlanNode> all;
            all = joinInfos.stream().flatMap(i -> i.getNodes().stream()).collect(toSet());
            checkArgument(nodes.equals(all), "There are extraneous nodes in the join path");
        }

    }

    public JoinPath(@Nonnull IndexedSet<PlanNode> allNodes, @Nonnull JoinInfo... infos) {
        this(allNodes, ImmutableList.copyOf(infos));
    }

    public JoinPath(@Nonnull IndexedSet<PlanNode> allNodes,
                    @Nonnull PlanNode node) {
        checkArgument(allNodes.contains(node), "node ∉ allNodes");
        joinInfos = ImmutableList.of();
        nodes = allNodes.immutableSubset(node);
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

    public @Nonnull ImmutableIndexedSubset<PlanNode> getNodes() {
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
