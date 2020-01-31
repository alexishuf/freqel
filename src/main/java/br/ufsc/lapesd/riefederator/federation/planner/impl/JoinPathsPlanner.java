package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.EmptyNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.util.UndirectedIrreflexiveArrayGraph;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class JoinPathsPlanner implements Planner {
    private static final int PATHS_PAR_THRESHOLD = 6;
    private @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public JoinPathsPlanner(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull CQuery query, @Nonnull Collection<QueryNode> queryNodes){
        checkArgument(!queryNodes.isEmpty(), "Cannot plan without QueryNodes!");
        if (query.isEmpty())
            return new EmptyNode(query.streamTerms(Var.class).map(Var::getName).collect(toList()));
        if (JoinPathsPlanner.class.desiredAssertionStatus()) {
            Set<Triple> full = query.getMatchedTriples();
            if (!queryNodes.stream().allMatch(n -> full.containsAll(n.getMatchedTriples())))
                throw new IllegalArgumentException("Some QueryNodes match triples not in query");
        }

        List<PlanNode> leaves = groupNodes(queryNodes);
        JoinGraph g = new JoinGraph(leaves);
        Set<Path> paths = new HashSet<>(leaves.size());
        getPaths(query, g, leaves, paths);
        boolean parallel = paths.size() > PATHS_PAR_THRESHOLD;
        List<PlanNode> plans = (parallel ? paths.parallelStream() : paths.stream())
                .filter(Path::hasJoins).map(Path::getJoinInfos)
                .map(joinOrderPlanner::plan).collect(toList());

        MultiQueryNode.Builder builder = MultiQueryNode.builder();
        builder.addAll(plans);
        paths.stream().filter(Path::isWhole).map(Path::getWhole).forEach(builder::add);
        return builder.build();
    }

    @VisibleForTesting
    @Nonnull List<PlanNode> groupNodes(@Nonnull Collection<QueryNode> queryNodes) {
        ListMultimap<JoinInterface, QueryNode> mm;
        mm = MultimapBuilder.hashKeys(queryNodes.size()).arrayListValues().build();

        for (QueryNode node : queryNodes)
            mm.put(new JoinInterface(node), node);

        List<PlanNode> list = new ArrayList<>();
        for (JoinInterface key : mm.keySet()) {
            Collection<QueryNode> nodes = mm.get(key);
            assert !nodes.isEmpty();
            if (nodes.size() > 1)
                list.add(MultiQueryNode.builder().addAll(nodes).build());
            else
                list.add(nodes.iterator().next());
        }
        return list;
    }

    @VisibleForTesting
    void getPaths(@Nonnull CQuery query, @Nonnull JoinGraph g, @Nonnull List<PlanNode> queryNodes,
                  @Nonnull Set<Path> paths) {
        int totalTriples = query.getMatchedTriples().size();
        ArrayDeque<State> stack = new ArrayDeque<>(queryNodes.size()*2);
        queryNodes.forEach(n -> stack.push(State.start(n)));
        while (!stack.isEmpty()) {
            State state = stack.pop();
            if (state.matchedTriples == totalTriples) {
                if (!state.hasInputs())
                    paths.add(state.toPath());
            } else {
                g.forEachNeighbor(state.node, (info, node) -> {
                    State next = state.advance(info, node);
                    if (next != null)
                        stack.push(next);
                });
            }
        }
    }

    @VisibleForTesting
    static class State {
        final @Nonnull PlanNode node;
        final @Nullable JoinInfo joinInfo;
        final @Nullable State ancestor;
        final int depth;
        final int matchedTriples;

        private State(@Nonnull PlanNode node, @Nullable JoinInfo joinInfo,
                      @Nullable State ancestor, int matchedTriples) {
            this.node = node;
            this.joinInfo = joinInfo;
            this.ancestor = ancestor;
            this.depth = ancestor == null ? 0 : ancestor.depth+1;
            this.matchedTriples = matchedTriples;
        }
        static @Nonnull State start(@Nonnull PlanNode start) {
            return new State(start, null, null, start.getMatchedTriples().size());
        }
        @Nullable State advance(@Nonnull JoinInfo info, PlanNode nextNode) {
            assert info.getLeft() == nextNode || info.getRight() == nextNode;
            assert info.getLeft() == node     || info.getRight() == node    ;
            int novel = countNovelMatchedTriples(nextNode);
            if (novel == 0)
                return null;
            return new State(nextNode, info, this, matchedTriples + novel);
        }
        boolean hasInputs() {
            return joinInfo == null ? node.hasInputs() : !joinInfo.getPendingInputs().isEmpty();
        }

        @Nonnull Path toPath() {
            if (depth == 0)
                return new Path(node);
            List<JoinInfo> list = new ArrayList<>(depth);
            for (State s = this; s != null; s = s.ancestor) {
                if (s.joinInfo != null) list.add(s.joinInfo);
                else                    assert s.ancestor == null;
            }
            return new Path(list);
        }

        private int countNovelMatchedTriples(PlanNode nextNode) {
            Set<Triple> candidates = nextNode.getMatchedTriples();
            if (candidates.size() == 1) {
                Triple triple = candidates.iterator().next();
                for (State s = this; s != null; s = s.ancestor) {
                    if (s.node.getMatchedTriples().contains(triple))
                        return 0;
                }
                return 1;
            } else {
                Set<Triple> pending = new HashSet<>(candidates);
                for (State s = this; !pending.isEmpty() && s != null; s = s.ancestor)
                    pending.removeAll(s.node.getMatchedTriples());
                return pending.size();
            }
        }
    }

    static class Path {
        private List<JoinInfo> joinInfos;
        private Set<PlanNode> nodes;
        private int hash = 0;

        Path(@Nonnull List<JoinInfo> joinInfos) {
            checkArgument(!joinInfos.isEmpty());
            this.joinInfos = joinInfos;
            nodes = new HashSet<>(joinInfos.size());
            Iterator<JoinInfo> it = joinInfos.iterator();
            JoinInfo current = it.next();
            nodes.add(current.getLeft());
            nodes.add(current.getRight());
            for (JoinInfo last = current; it.hasNext(); last = current)
                nodes.add((current = it.next()).getOppositeToLinked(last));
        }

        Path(@Nonnull PlanNode node) {
            joinInfos = Collections.emptyList();
            nodes = Collections.singleton(node);
            hash = node.hashCode();
        }

        public @Nonnull List<JoinInfo> getJoinInfos() {
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

        public @Nonnull Set<PlanNode> getNodes() {
            return nodes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Path)) return false;
            Path other = (Path) o;
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


    @VisibleForTesting
    static class JoinGraph extends UndirectedIrreflexiveArrayGraph<PlanNode, JoinInfo> {
        public JoinGraph(@Nonnull List<PlanNode> nodes) {
            super(JoinInfo.class, null, nodes);
        }

        @Override
        protected @Nullable JoinInfo weigh(@Nonnull PlanNode l, @Nonnull PlanNode r) {
            JoinInfo info = JoinInfo.getPlainJoinability(l, r);
            return info.isValid() ? info : null;
        }
    }
}
