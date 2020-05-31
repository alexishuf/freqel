package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinComponent;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.SubPathAggregation;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.ImmutableIndexedSubset;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class JoinPathsPlanner implements Planner {
    private static final Logger logger = LoggerFactory.getLogger(JoinPathsPlanner.class);
    private static final int PATHS_PAR_THRESHOLD = 10;
    private final @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public JoinPathsPlanner(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public boolean allowJoinDisconnected() {
        return false;
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull CQuery query, @Nonnull Collection<QueryNode> qns){
        checkArgument(!qns.isEmpty(), "Cannot plan without QueryNodes!");
        if (query.isEmpty())
            return new EmptyNode(query);
        IndexedSet<Triple> full = IndexedSet.fromDistinctCopy(query.getMatchedTriples());
        if (JoinPathsPlanner.class.desiredAssertionStatus()) {
            checkArgument(qns.stream().allMatch(n -> full.containsAll(n.getMatchedTriples())),
                          "Some QueryNodes match triples not in query");
        }
        if (!satisfiesAll(full, qns, query))
            return new EmptyNode(query);
        /* throw instead of EmptyNode bcs allowJoinDisconnected(). Failing here means someone
           built a Federation using incompatible components. This is likely a bug in the
           Federation creator */
        NaiveOuterPlanner outerPlanner = new NaiveOuterPlanner();
        PlanNode plan = outerPlanner.plan(query);
        if (!(plan instanceof ComponentNode)) {
            logger.warn("JoinPathsPlanner was designed for handling conjunctive queries, yet " +
                        "the given query requires Cartesian products or unions. Will try to " +
                        "continue, but planning may fail. Query: {}", query);
            Map<PlanNode, PlanNode> replacements = new HashMap<>();
            TreeUtils.streamPreOrder(plan).filter(ComponentNode.class::isInstance).forEach(n -> {
                IndexedSet<Triple> component = ((ComponentNode) n).getQuery().getSet();
                List<QueryNode> relevant = qns.stream()
                        .filter(qn -> component.containsAny(qn.getMatchedTriples()))
                        .collect(toList());
                PlanNode subPlan = plan(relevant, component);
                if (subPlan instanceof EmptyNode)
                    logger.info("Unsatisfiable query component: {} ", component);
                replacements.put(n, subPlan);
            });
            if (replacements.values().stream().anyMatch(EmptyNode.class::isInstance)) {
                logger.info("A conjunctive query component is unsatisfiable. JoinPathsPlanner " +
                            "used NaiveOuterPlanner to identify components. Query: {}.", query);
                return new EmptyNode(query);
            }
            return TreeUtils.replaceNodes(plan, replacements);
        } else {
            PlanNode root = plan(qns, full);
            if (root == null) {
                logger.info("No join path across endpoints for join-connected query {}. " +
                        "Returning EmptyNode", query);
                return new EmptyNode(query);
            }
            return root;
        }
    }

    private void assertValidJoinComponents(@Nonnull Collection<JoinComponent> components,
                                           @Nonnull IndexedSet<Triple> full) {
        if (!JoinPathsPlanner.class.desiredAssertionStatus())
            return;
        for (JoinComponent component : components) {
            for (PlanNode i : component.getNodes()) {
                Set<Triple> iMatched = i.getMatchedTriples();
                for (PlanNode j : component.getNodes()) {
                    if (i == j) continue;
                    Set<Triple> jMatched = j.getMatchedTriples();
                    if (iMatched.containsAll(jMatched)) {
                        String message = "Node " + j + " in a JoinComponent is subsumed by " + i;
                        throw new AssertionError(message);
                    }
                }
            }
            assert satisfiesAll(full, component.getNodes(), null)
                    : "Not all "+full.size()+" triples are satisfied by JoinComponent"+component;
        }
    }

    private @Nullable PlanNode plan(@Nonnull Collection<QueryNode> qns,
                                    @Nonnull IndexedSet<Triple> triples) {
        IndexedSet<PlanNode> leaves = groupNodes(qns);
        JoinGraph g = new JoinGraph(leaves);
        List<JoinComponent> pathsSet = getPaths(triples, g);
        assertValidJoinComponents(pathsSet, triples);
        removeAlternativePaths(pathsSet);
        assertValidJoinComponents(pathsSet, triples);
        if (pathsSet.isEmpty())
            return null;

        SubPathAggregation aggregation = SubPathAggregation.aggregate(g, pathsSet, joinOrderPlanner);
        JoinGraph g2 = aggregation.getGraph();
        List<JoinComponent> aggregatedPaths = aggregation.getJoinComponents();
        assertValidJoinComponents(aggregatedPaths, triples);
        boolean parallel = pathsSet.size() > PATHS_PAR_THRESHOLD;
        MultiQueryNode.Builder builder = MultiQueryNode.builder();
        builder.addAll((parallel ? aggregatedPaths.parallelStream() : aggregatedPaths.stream())
                .map(p -> joinOrderPlanner.plan(g2, p.getNodes()))
                .collect(toList()));
        return builder.buildIfMulti();
    }

    @VisibleForTesting
    void removeAlternativePaths(@Nonnull List<JoinComponent> paths) {
        IndexedSet<PlanNode> set = getNodesIndexedSetFromPaths(paths);
        BitSet marked = new BitSet(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            IndexedSubset<PlanNode> outer = set.subset(paths.get(i).getNodes());
            assert outer.size() == paths.get(i).getNodes().size();
            for (int j = i+1; j < paths.size(); j++) {
                if (marked.get(j))
                    continue;
                IndexedSubset<PlanNode> inner = set.subset(paths.get(j).getNodes());
                assert inner.size() == paths.get(j).getNodes().size();
                if (outer.equals(inner))
                    marked.set(j);
            }
        }

        for (int i = marked.length(); (i = marked.previousSetBit(i-1)) >= 0; )
            paths.remove(i);
    }

    @VisibleForTesting
    @Nonnull IndexedSet<PlanNode> getNodesIndexedSetFromPaths(@Nonnull List<JoinComponent> paths) {
        List<PlanNode> list = new ArrayList<>();
        Map<PlanNode, Integer> n2idx = new HashMap<>();
        Multimap<Set<Triple>, QueryNode> mm = MultimapBuilder.hashKeys().arrayListValues().build();
        for (JoinComponent path : paths) {
            for (PlanNode node : path.getNodes()) {
                if (node instanceof QueryNode && !n2idx.containsKey(node)) {
                    QueryNode qn = (QueryNode) node;
                    TPEndpoint endpoint = qn.getEndpoint();
                    int canonIdx = -1;
                    for (QueryNode candidate : mm.get(qn.getQuery().getSet())) {
                        if (candidate.getEndpoint().isAlternative(endpoint)) {
                            canonIdx = n2idx.get(candidate);
                            assert canonIdx >= 0 && canonIdx < list.size();
                            break;
                        }
                    }
                    if (canonIdx == -1) {
                        n2idx.put(qn, list.size());
                        list.add(qn);
                        mm.put(qn.getQuery().getSet(), qn);
                    } else {
                        n2idx.put(qn, canonIdx);
                    }
                }
            }
        }
        for (JoinComponent path : paths) {
            for (PlanNode node : path.getNodes()) {
                if (node instanceof QueryNode || n2idx.containsKey(node)) continue;
                assert list.size() == n2idx.size();
                n2idx.put(node, n2idx.size());
                list.add(node);
            }
        }
        return IndexedSet.fromMap(n2idx, list);
    }

    private boolean satisfiesAll(@Nonnull IndexedSet<Triple> all,
                                 @Nonnull Collection<? extends PlanNode> nodes,
                                 @Nullable CQuery query) {
        IndexedSubset<Triple> subset = all.emptySubset();
        for (PlanNode node : nodes)
            subset.union(node.getMatchedTriples());
        if (subset.size() != all.size()) {
            IndexedSubset<Triple> missing = all.fullSubset();
            missing.removeAll(subset);
            if (query != null) {
                logger.info("QueryNodes miss triples {}. Full query was {}. Returning EmptyNode",
                            missing, query);
            } else {
                logger.info("QueryNodes miss triples {}.", missing);
            }
            return false;
        }
        return true;
    }

    @VisibleForTesting
    @Nonnull IndexedSet<PlanNode> groupNodes(@Nonnull Collection<QueryNode> queryNodes) {
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
        return IndexedSet.fromDistinct(list);
    }

//    @VisibleForTesting
//    List<JoinComponent> getPaths(@Nonnull IndexedSet<Triple> full, @Nonnull JoinGraph g) {
//        Set<JoinComponent> paths = new HashSet<>(g.size());
//        int totalTriples = full.size();
//        IndexedSet<PlanNode> nodes = g.getNodes();
//        ArrayDeque<State> stack = new ArrayDeque<>(nodes.size()*2);
//        nodes.forEach(n -> stack.push(State.start(full, n)));
//        while (!stack.isEmpty()) {
//            State state = stack.pop();
//            if (state.matched.size() == totalTriples) {
//                if (!state.hasInputs()) //ignore join paths that leave pending inputs
//                    paths.add(state.toPath(nodes));
//                else
//                    logger.debug("Discarding path with pending inputs {}", state.toPath(nodes));
//            } else {
//                g.forEachNeighbor(state.node, (info, node) -> {
//                    State next = state.advance(info, node);
//                    if (next != null)
//                        stack.push(next);
//                });
//            }
//        }
//        return new ArrayList<>(paths);
//    }

    @VisibleForTesting
    List<JoinComponent> getPaths(@Nonnull IndexedSet<Triple> full, @Nonnull JoinGraph g) {
        if (g.isEmpty())
            return Collections.emptyList();
        PathsContext context = new PathsContext(full, g);
        context.run();
        return context.result;
    }


    private final static class PathsContext {
        private final List<JoinComponent> result;
        private final ArrayDeque<State> queue = new ArrayDeque<>();
        private final @Nonnull IndexedSet<Triple> full;
        private final @Nonnull JoinGraph g;
        private final @Nonnull Cache<State, Boolean> recent;

        private static final class State {
            final @Nonnull ImmutableIndexedSubset<PlanNode> nodes;
            final @Nonnull ImmutableIndexedSubset<Triple> matched;
            final int[] tripleOccurrences;

            private State(@Nonnull ImmutableIndexedSubset<PlanNode> nodes,
                          @Nonnull ImmutableIndexedSubset<Triple> matched,
                          int[] tripleOccurrences) {
                this.nodes = nodes;
                this.matched = matched;
                this.tripleOccurrences = tripleOccurrences;
            }

            private static int[] initTripleOccurrences(ImmutableIndexedSubset<Triple> matched) {
                int[] occurrences = new int[matched.getParent().size()];
                BitSet bs = matched.getBitSet();
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
                    occurrences[i] = 1;
                return occurrences;
            }

            public State(@Nonnull ImmutableIndexedSubset<PlanNode> nodes,
                         @Nonnull ImmutableIndexedSubset<Triple> matched) {
                this(nodes, matched, initTripleOccurrences(matched));
            }

            @Override
            public @Nonnull String toString() {
                return nodes.getBitSet().toString();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof State)) return false;
                State state = (State) o;
                assert nodes.equals(state.nodes) == matched.equals(state.matched);
                return nodes.equals(state.nodes);
            }

            @Override
            public int hashCode() {
                return Objects.hash(nodes);
            }

            @Nonnull JoinComponent toJoinComponent() {
                // invariant: no node subsumes another
                assert nodes.stream().noneMatch(
                        n -> nodes.stream().anyMatch(
                                n2 -> n != n2 && n.getMatchedTriples()
                                                  .containsAll(n2.getMatchedTriples())));
                // invariant: matched is the union of all matched triples
                assert nodes.stream().flatMap(n -> n.getMatchedTriples().stream())
                            .collect(toSet()).equals(matched);
                return new JoinComponent(nodes.getParent(), nodes); //share nodes
            }

            boolean hasConflictingNode(@Nonnull IndexedSubset<Triple> candidateMatched) {
                BitSet bs = candidateMatched.getBitSet();
                IndexedSet<Triple> all = candidateMatched.getParent();
                assert tripleOccurrences.length == all.size();
                outer:
                for (PlanNode old : nodes) {
                    for (Triple triple : old.getMatchedTriples()) {
                        int idx = all.indexOf(triple);
                        assert idx >= 0;
                        if (tripleOccurrences[idx] == 1 && !bs.get(idx))
                            continue outer;
                    }
                    return true; //all triples found only in old are also in candidateMatched
                }
                return false; //every old node still contributes at least triple
            }

            @Nullable State tryAdvance(@Nonnull PlanNode node) {
                IndexedSubset<Triple> nodeMatched;
                nodeMatched = matched.getParent().subset(node.getMatchedTriples());
                assert nodeMatched.size() == node.getMatchedTriples().size();

                ImmutableIndexedSubset<Triple> novel = matched.createUnion(nodeMatched);
                if (novel.size() == matched.size())
                    return null; //no new triples
                if (hasConflictingNode(nodeMatched))
                    return null; // adding the node would make another useless

                // for every triple in the new node, increment its occurrence count
                int[] occurrences = Arrays.copyOf(tripleOccurrences, tripleOccurrences.length);
                BitSet bs = nodeMatched.getBitSet();
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
                    ++occurrences[i];
                return new State(nodes.createAdding(node), novel, occurrences);
            }
        }

        public PathsContext(@Nonnull IndexedSet<Triple> full, @Nonnull JoinGraph g) {
            this.full = full;
            this.g = g;
            result = new ArrayList<>();
            recent = CacheBuilder.newBuilder().maximumSize(4096).initialCapacity(512).build();
        }

        void run() {
            assert !g.isEmpty();
            assert queue.isEmpty();
            for (PlanNode node : g.getNodes())
                queue.add(createState(node));
            while (!queue.isEmpty()) {
                State state = queue.remove();
                if (checkRecent(state))
                    continue;
                if (state.matched.size() == full.size()) {
                    assert state.matched.equals(full); // size() works bcs full is shared as parent
                    Set<String> nonIn = state.nodes.stream()
                            .flatMap(n -> n.getStrictResultVars().stream()).collect(toSet());
                    if (state.nodes.stream().allMatch(n -> nonIn.containsAll(n.getRequiredInputVars()))) {
                        JoinComponent component = state.toJoinComponent();
                        assert component.getNodes().getParent().equals(g.getNodes());
                        result.add(component);
                        continue;
                    }
                }
                advance(state);
            }
            assert result.stream().distinct().count() == result.size(); // no duplicates!
        }

        private State createState(@Nonnull PlanNode node) {
            return new State(g.getNodes().immutableSubset(node),
                             full.immutableSubset(node.getMatchedTriples()));
        }

        private boolean checkRecent(@Nonnull State state) {
            if (recent.getIfPresent(state) != null)
                return true; //was recently visited
            recent.put(state, true);
            return false; //not recent
        }

        private void advance(@Nonnull State state) {
            for (PlanNode node : state.nodes) {
                g.forEachNeighbor(node, (i, neighbor) -> {
                    if (state.nodes.contains(neighbor))
                        return; // do not add something it already has
                    State next = state.tryAdvance(neighbor);
                    if (next != null) {
                        assert next.nodes.size() > state.nodes.size(); //invariant
                        queue.add(next);
                    }
                });
            }
        }
    }

}
