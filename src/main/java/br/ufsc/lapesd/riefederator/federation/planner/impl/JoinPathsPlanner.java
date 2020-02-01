package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinPath;
import br.ufsc.lapesd.riefederator.federation.tree.EmptyNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class JoinPathsPlanner implements Planner {
    private static final Logger logger = LoggerFactory.getLogger(JoinPathsPlanner.class);
    private static final int PATHS_PAR_THRESHOLD = 6;
    private @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public JoinPathsPlanner(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull CQuery query, @Nonnull Collection<QueryNode> qns){
        checkArgument(!qns.isEmpty(), "Cannot plan without QueryNodes!");
        if (query.isEmpty())
            return new EmptyNode(query.streamTerms(Var.class).map(Var::getName).collect(toList()));
        IndexedSet<Triple> full = IndexedSet.fromDistinctCopy(query.getMatchedTriples());
        if (JoinPathsPlanner.class.desiredAssertionStatus()) {
            checkArgument(qns.stream().allMatch(n -> full.containsAll(n.getMatchedTriples())),
                          "Some QueryNodes match triples not in query");
        }
        if (!satisfiesAll(full, qns)) {
            logger.info("QueryNodes miss some triples in query {}, returning EmptyNode", query);
            return EmptyNode.createFor(query);
        }

        List<PlanNode> leaves = groupNodes(qns);
        JoinGraph g = new JoinGraph(leaves);
        Set<JoinPath> paths = new HashSet<>(leaves.size());
        getPaths(full, g, paths);
        boolean parallel = paths.size() > PATHS_PAR_THRESHOLD;
        List<PlanNode> plans = (parallel ? paths.parallelStream() : paths.stream())
                .map(p -> p.isWhole() ? p.getWhole() : joinOrderPlanner.plan(p.getJoinInfos()))
                .collect(toList());

        MultiQueryNode.Builder builder = MultiQueryNode.builder();
        builder.addAll(plans);
        paths.stream().filter(JoinPath::isWhole).map(JoinPath::getWhole).forEach(builder::add);
        return builder.build();
    }

    private boolean satisfiesAll(@Nonnull IndexedSet<Triple> all,
                                 @Nonnull Collection<QueryNode> qns) {
        IndexedSubset<Triple> subset = all.emptySubset();
        for (QueryNode qn : qns)
            subset.union(qn.getMatchedTriples());
        return subset.size() == all.size();
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
    void getPaths(@Nonnull IndexedSet<Triple> full, @Nonnull JoinGraph g,
                  @Nonnull Set<JoinPath> paths) {
        int totalTriples = full.size();
        List<PlanNode> nodes = g.getNodes();
        ArrayDeque<State> stack = new ArrayDeque<>(nodes.size()*2);
        nodes.forEach(n -> stack.push(State.start(full, n)));
        while (!stack.isEmpty()) {
            State state = stack.pop();
            if (state.matched.size() == totalTriples) {
                if (!state.hasInputs()) //ignore join paths that leave pending inputs
                    paths.add(state.toPath());
                else
                    logger.debug("Discarding path with pending inputs {}", state.toPath());
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
        final IndexedSubset<Triple> matched;

        private State(@Nonnull PlanNode node, @Nullable JoinInfo joinInfo,
                      @Nullable State ancestor,
                      @Nonnull IndexedSubset<Triple> matched) {
            this.node = node;
            this.joinInfo = joinInfo;
            this.ancestor = ancestor;
            this.depth = ancestor == null ? 0 : ancestor.depth+1;
            this.matched = matched;
        }
        static @Nonnull State start(@Nonnull IndexedSet<Triple> all, @Nonnull PlanNode start) {
            IndexedSubset<Triple> matchedTriples = all.subset(start.getMatchedTriples());
            return new State(start, null, null, matchedTriples);
        }
        @Nullable State advance(@Nonnull JoinInfo info, PlanNode nextNode) {
            assert nextNode != node;
            assert info.getLeft() == nextNode || info.getRight() == nextNode;
            assert info.getLeft() == node     || info.getRight() == node    ;

            IndexedSubset<Triple> novelMatched = matched.createUnion(nextNode.getMatchedTriples());
            assert novelMatched.size() >= matched.size();
            if (novelMatched.size() == matched.size())
                return null; // no new triples satisfied
            return new State(nextNode, info, this, novelMatched);
        }
        boolean hasInputs() {
            return joinInfo == null ? node.hasInputs() : !joinInfo.getPendingInputs().isEmpty();
        }

        @Nonnull JoinPath toPath() {
            if (depth == 0)
                return new JoinPath(node);
            //noinspection UnstableApiUsage
            ImmutableList.Builder<JoinInfo> builder = ImmutableList.builderWithExpectedSize(depth);
            for (State s = this; s != null; s = s.ancestor) {
                if (s.joinInfo != null) builder.add(s.joinInfo);
                else                    assert s.ancestor == null;
            }
            return new JoinPath(builder.build());
        }
    }

}
