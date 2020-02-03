package br.ufsc.lapesd.riefederator.federation.planner.impl.paths;

import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.ImmutableIndexedSubset;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
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
        checkArgument(joinInfos.stream().allMatch(JoinInfo::isValid),
                      "There are invalid JoinInfos in the path");
        checkArgument(joinInfos.stream().allMatch(j -> allNodes.containsAll(j.getNodes())),
                      "There are operands of JoinInfos ∉ allNodes");

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

    public static @Nullable JoinPath findPath(@Nonnull JoinGraph joinGraph,
                                              @Nonnull Collection<PlanNode> nodes) {
        checkArgument(!nodes.isEmpty(), "Cannot build a plan without any nodes");
        checkArgument(nodes.size() > 1, "Needs at least two nodes to have a join");
        checkArgument(joinGraph.getNodes().containsAll(nodes), "JoinGraph misses some nodes");
        checkArgument(nodes.stream().noneMatch(Objects::isNull), "Null nodes not allowed");

        if (JoinPath.class.desiredAssertionStatus()) { //skip more expensive checks
            checkArgument(new HashSet<>(nodes).size() == nodes.size(), "Non-unique nodes");

            IndexedSet<Triple> allTriples = IndexedSet.from(nodes.stream()
                    .flatMap(n -> n.getMatchedTriples().stream()).collect(toList()));
            List<IndexedSubset<Triple>> matched = nodes.stream()
                    .map(n -> allTriples.subset(n.getMatchedTriples())).collect(toList());
            for (int i = 0; i < matched.size(); i++) {
                Set<Triple> inner = matched.get(i);
                for (int j = i + 1; j < matched.size(); j++) {
                    checkArgument(!matched.get(j).containsAll(inner),
                            "There are nodes whose getMatchedTriples() subsume another");
                }
            }
        }

        FindPathState state = new FindPathState(joinGraph, nodes);
        if (!state.findPath())
            return null;
        return new JoinPath(joinGraph.getNodes(), ImmutableList.copyOf(state.path));
    }

    private static class FindPathState {
        private final JoinGraph joinGraph;
        private final IndexedSubset<PlanNode> nodesSet, open;
        private final List<JoinInfo> path;
        private final int targetDepth;

        public FindPathState(@Nonnull JoinGraph joinGraph,
                             @Nonnull Collection<PlanNode> collection) {
            this.joinGraph = joinGraph;
            this.nodesSet = joinGraph.getNodes().subset(collection);
            this.open = joinGraph.getNodes().emptySubset();
            this.targetDepth = collection.size()-1;
            this.path = new ArrayList<>(targetDepth);
            for (int i = 0; i < targetDepth; i++)
                this.path.add(null);
        }

        public boolean findPath() {
            for (PlanNode node : nodesSet) {
                if (findPath(node, 0)) return true;
            }
            return false;
        }

        public boolean findPath(@Nonnull PlanNode node, int depth) {
            if (!open.add(node))
                return false;
            else if (depth == targetDepth)
                return true;

            boolean[] got = {false};
            joinGraph.forEachNeighbor(node, (i, n) -> {
                if (!got[0] && nodesSet.contains(n)) {
                    if ((got[0] = findPath(n, depth+1)))
                        path.set(depth, i);
                }
            });
            if (!got[0])
                open.remove(node);
            return got[0];
        }
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
