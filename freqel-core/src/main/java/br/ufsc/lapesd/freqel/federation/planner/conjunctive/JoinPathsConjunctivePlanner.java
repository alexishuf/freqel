package br.ufsc.lapesd.freqel.federation.planner.conjunctive;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.JoinInterface;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.cardinality.InnerCardinalityComputer;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.PrePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.paths.JoinComponent;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.paths.SubPathAggregation;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.Bitset;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.ImmIndexSubset;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
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
import static java.util.stream.Collectors.*;

public class JoinPathsConjunctivePlanner implements ConjunctivePlanner {
    private static final Logger logger = LoggerFactory.getLogger(JoinPathsConjunctivePlanner.class);
    private static final int PATHS_PAR_THRESHOLD = 10;
    private final @Nonnull JoinOrderPlanner joinOrderPlanner;
    private final @Nonnull InnerCardinalityComputer innerCardComputer;
    private final @Nonnull PrePlanner prePlanner;

    @Inject
    public JoinPathsConjunctivePlanner(@Nonnull JoinOrderPlanner joinOrderPlanner,
                                       @Nonnull InnerCardinalityComputer innerCardComputer,
                                       @Nonnull PrePlanner prePlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
        this.innerCardComputer = innerCardComputer;
        this.prePlanner = prePlanner;
    }

    @Override
    public String toString() {
        return "JoinPathsPlanner";
    }

    @Override
    public @Nonnull Op plan(@Nonnull CQuery query, @Nonnull Collection<Op> qns){
        if (query.isEmpty()) //empty query
            return new EmptyOp(new QueryOp(query));
        if (qns.isEmpty()) {
            logger.debug("No subqueries (lack of sources?). Query: \"\"\"{}\"\"\"", query);
            return new EmptyOp(new QueryOp(query));
        }
        IndexSet<Triple> full = FullIndexSet.fromDistinctCopy(query.attr().matchedTriples());
        if (JoinPathsConjunctivePlanner.class.desiredAssertionStatus()) {
            checkArgument(qns.stream().allMatch(n -> full.containsAll(n.getMatchedTriples())),
                          "Some QueryNodes match triples not in query");
        }
        if (!satisfiesAll(full, qns, query))
            return new EmptyOp(new QueryOp(query));
        /* throw instead of EmptyNode bcs allowJoinDisconnected(). Failing here means someone
           built a Federation using incompatible components. This is likely a bug in the
           Federation creator */
        if (!query.attr().isJoinConnected()) {
            Op plan = prePlanner.plan(new QueryOp(query));
            logger.warn("JoinPathsPlanner was designed for handling conjunctive queries, yet " +
                        "the given query requires Cartesian products or unions. Will try to " +
                        "continue, but planning may fail. Query: {}", query);
            plan = TreeUtils.replaceNodes(plan, innerCardComputer, op -> {
                if (!(op.getClass().equals(QueryOp.class))) return op;
                IndexSet<Triple> component = ((QueryOp) op).getQuery().attr().getSet();
                List<Op> relevant = qns.stream()
                        .filter(qn -> component.containsAny(qn.getMatchedTriples()))
                        .collect(toList());
                Op subPlan = plan(relevant, component);
                if (subPlan == null)
                    subPlan = new EmptyOp(op.getResultVars());
                assert subPlan != op;
                return subPlan;
            });
            if (plan.getCardinality().equals(Cardinality.EMPTY)) {
                logger.info("A conjunctive query component is unsatisfiable. JoinPathsPlanner " +
                            "used NaiveOuterPlanner to identify components. Query: {}.", query);
                return new EmptyOp(new QueryOp(query));
            }
            return plan;
        } else {
            Op root = plan(qns, full);
            if (root == null) {
                logger.info("No join path across endpoints for join-connected query {}. " +
                            "Returning EmptyNode", query);
                return new EmptyOp(new QueryOp(query));
            }
            return root;
        }
    }

    private void assertValidJoinComponents(@Nonnull Collection<JoinComponent> components,
                                           @Nonnull IndexSet<Triple> full) {
        if (!JoinPathsConjunctivePlanner.class.desiredAssertionStatus())
            return;
        for (JoinComponent component : components) {
            for (Op i : component.getNodes()) {
                Set<Triple> iMatched = i.getMatchedTriples();
                for (Op j : component.getNodes()) {
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

    private @Nullable Op plan(@Nonnull Collection<Op> qns,
                              @Nonnull IndexSet<Triple> triples) {
        RefIndexSet<Op> leaves = groupNodes(qns);
        JoinGraph g = new ArrayJoinGraph(leaves);
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
        UnionOp.Builder builder = UnionOp.builder();
        builder.addAll((parallel ? aggregatedPaths.parallelStream() : aggregatedPaths.stream())
                .map(p -> joinOrderPlanner.plan(g2, p.getNodes()))
                .collect(toList()));
        return builder.build();
    }

    @VisibleForTesting
    void removeAlternativePaths(@Nonnull List<JoinComponent> paths) {
        RefIndexSet<Op> set = getNodesIndexedSetFromPaths(paths);
        BitSet marked = new BitSet(paths.size());
        for (int i = 0; i < paths.size(); i++) {
            IndexSubset<Op> outer = set.subset(paths.get(i).getNodes());
            assert outer.size() == paths.get(i).getNodes().size();
            for (int j = i+1; j < paths.size(); j++) {
                if (marked.get(j))
                    continue;
                IndexSubset<Op> inner = set.subset(paths.get(j).getNodes());
                assert inner.size() == paths.get(j).getNodes().size();
                if (outer.equals(inner))
                    marked.set(j);
            }
        }

        for (int i = marked.length(); (i = marked.previousSetBit(i-1)) >= 0; )
            paths.remove(i);
    }

    @VisibleForTesting
    @Nonnull RefIndexSet<Op> getNodesIndexedSetFromPaths(@Nonnull List<JoinComponent> paths) {
        List<Op> list = new ArrayList<>();
        IdentityHashMap<Op, Integer> n2idx = new IdentityHashMap<>();
        Multimap<Set<Triple>, EndpointQueryOp> mm = MultimapBuilder.hashKeys().arrayListValues().build();
        for (JoinComponent path : paths) {
            for (Op node : path.getNodes()) {
                if (node instanceof EndpointQueryOp && !n2idx.containsKey(node)) {
                    EndpointQueryOp qn = (EndpointQueryOp) node;
                    TPEndpoint endpoint = qn.getEndpoint();
                    int canonIdx = -1;
                    for (EndpointQueryOp candidate : mm.get(qn.getQuery().attr().getSet())) {
                        if (candidate.getEndpoint().isAlternative(endpoint)) {
                            canonIdx = n2idx.get(candidate);
                            assert canonIdx >= 0 && canonIdx < list.size();
                            break;
                        }
                    }
                    if (canonIdx == -1) {
                        n2idx.put(qn, list.size());
                        list.add(qn);
                        mm.put(qn.getQuery().attr().getSet(), qn);
                    } else {
                        n2idx.put(qn, canonIdx);
                    }
                }
            }
        }
        for (JoinComponent path : paths) {
            for (Op node : path.getNodes()) {
                if (node instanceof EndpointQueryOp || n2idx.containsKey(node)) continue;
                assert list.size() == n2idx.size();
                n2idx.put(node, n2idx.size());
                list.add(node);
            }
        }
        return new RefIndexSet<>(n2idx, list);
    }

    private boolean satisfiesAll(@Nonnull IndexSet<Triple> all,
                                 @Nonnull Collection<? extends Op> nodes,
                                 @Nullable CQuery query) {
        IndexSubset<Triple> subset = all.emptySubset();
        for (Op node : nodes)
            subset.addAll(node.getMatchedTriples());
        if (subset.size() != all.size()) {
            IndexSubset<Triple> missing = all.fullSubset();
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
    @Nonnull RefIndexSet<Op> groupNodes(@Nonnull Collection<Op> queryNodes) {
        ListMultimap<JoinInterface, Op> mm;
        mm = MultimapBuilder.hashKeys(queryNodes.size()).arrayListValues().build();

        for (Op node : queryNodes)
            mm.put(new JoinInterface(node), node);

        IdentityHashMap<Op, Integer> n2idx = new IdentityHashMap<>(mm.keySet().size());
        List<Op> list = new ArrayList<>(mm.keySet().size());
        int size = 0;
        for (JoinInterface key : mm.keySet()) {
            Collection<Op> nodes = mm.get(key);
            assert !nodes.isEmpty();
            Op node = nodes.size() > 1 ? UnionOp.build(nodes) : nodes.iterator().next();
            list.add(node);
            n2idx.put(node, size++);
        }
        return new RefIndexSet<>(n2idx, list);
    }

    @VisibleForTesting
    List<JoinComponent> getPaths(@Nonnull IndexSet<Triple> full, @Nonnull JoinGraph g) {
        if (g.isEmpty())
            return Collections.emptyList();
        PathsContext context = new PathsContext(full, g);
        context.run();
        if (context.result.isEmpty() && logger.isInfoEnabled()) {
            StringBuilder b1 = new StringBuilder();
            context.missingSmall.forEach(s -> b1.append("  ").append(s).append('\n'));

            StringBuilder b2 = new StringBuilder();
            for (IndexSubset<Op> c : context.unresolvedInputsComponents) {
                b2.append("  {")
                        .append(c.stream().map(n -> String.valueOf(g.getNodes().indexOf(n)))
                                          .collect(joining(", ")))
                        .append("}\n");
            }

            StringBuilder b3 = new StringBuilder();
            g.getNodes().forEach(n ->
                    n.prettyPrint(b3.append("[[")
                                    .append(g.getNodes().indexOf(n)).append("]] "), "  "
                    ).append('\n'));
            logger.info("No effectively join-connected sub-component satisfies all {} " +
                        "triples of the query.\n{} triples are not satisfied by any " +
                        "sub-component: {}\nSmall sets of triples left unsatisfied by some " +
                        "sub-components: {}\nComponents with unresolved inputs (see [[n]] " +
                        "below for nodes):\n{}\nLeaf nodes considered for building effective " +
                        "join-connected sub-components:\n{}",
                        full.size(), context.globallyUnsatisfied.size(),
                        context.globallyUnsatisfied, b1.toString(), b2.toString(), b3.toString());
        }

        return context.result;
    }


    private final static class PathsContext {
        private final List<JoinComponent> result;
        private final ArrayDeque<State> queue;
        private final @Nonnull IndexSet<Triple> full;
        private final @Nonnull JoinGraph g;
        private final @Nonnull Cache<State, Boolean> recent;
        private final @Nonnull IndexSubset<Triple> globallyUnsatisfied;
        private final @Nonnull Set<IndexSubset<Triple>> missingSmall;
        private final @Nonnull Set<IndexSubset<Op>> unresolvedInputsComponents;

        private static final class State {
            final @Nonnull ImmIndexSubset<Op> nodes;
            final @Nonnull ImmIndexSubset<Triple> matched;
            final int[] tripleOccurrences;

            private State(@Nonnull ImmIndexSubset<Op> nodes,
                          @Nonnull ImmIndexSubset<Triple> matched,
                          int[] tripleOccurrences) {
                this.nodes = nodes;
                this.matched = matched;
                this.tripleOccurrences = tripleOccurrences;
            }

            private static int[] initTripleOccurrences(ImmIndexSubset<Triple> matched) {
                int[] occurrences = new int[matched.getParent().size()];
                Bitset bs = matched.getBitset();
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
                    occurrences[i] = 1;
                return occurrences;
            }

            public State(@Nonnull ImmIndexSubset<Op> nodes,
                         @Nonnull ImmIndexSubset<Triple> matched) {
                this(nodes, matched, initTripleOccurrences(matched));
            }

            @Override
            public @Nonnull String toString() {
                return nodes.getBitset().toString();
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
                return new JoinComponent((RefIndexSet<Op>) nodes.getParent(), nodes); //share nodes
            }

            boolean hasConflictingNode(@Nonnull IndexSubset<Triple> candidateMatched) {
                Bitset bs = candidateMatched.getBitset();
                IndexSet<Triple> all = candidateMatched.getParent();
                assert tripleOccurrences.length == all.size();
                outer:
                for (Op old : nodes) {
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

            @Nullable State tryAdvance(@Nonnull Op node) {
                IndexSubset<Triple> nodeMatched;
                nodeMatched = matched.getParent().subset(node.getMatchedTriples());
                assert nodeMatched.size() == node.getMatchedTriples().size();

                ImmIndexSubset<Triple> novel = matched.createImmutableUnion(nodeMatched);
                if (novel.size() == matched.size())
                    return null; //no new triples
                if (hasConflictingNode(nodeMatched))
                    return null; // adding the node would make another useless

                // for every triple in the new node, increment its occurrence count
                int[] occurrences = Arrays.copyOf(tripleOccurrences, tripleOccurrences.length);
                Bitset bs = nodeMatched.getBitset();
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1))
                    ++occurrences[i];
                return new State(nodes.createImmutableUnion(node), novel, occurrences);
            }
        }

        public PathsContext(@Nonnull IndexSet<Triple> full, @Nonnull JoinGraph g) {
            this.full = full;
            this.g = g;
            int nodesSize = g.getNodes().size();
            this.queue = new ArrayDeque<>(Math.max(nodesSize + nodesSize/2 + 1, 10));
            this.globallyUnsatisfied = full.fullSubset();
            this.result = new ArrayList<>();
            this.recent = CacheBuilder.newBuilder().maximumSize(4096).initialCapacity(512).build();
            this.missingSmall = new HashSet<>();
            this.unresolvedInputsComponents = new HashSet<>();
        }

        void run() {
            assert !g.isEmpty();
            assert queue.isEmpty();
            for (Op node : g.getNodes())
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
                    } else {
                        unresolvedInputsComponents.add(state.nodes);
                    }
                }
                advance(state);
            }
            assert result.stream().distinct().count() == result.size(); // no duplicates!
        }

        private State createState(@Nonnull Op node) {
            ImmIndexSubset<Triple> matched = full.immutableSubset(node.getMatchedTriples());
            globallyUnsatisfied.removeAll(matched);
            return new State(g.getNodes().immutableSubset(node), matched);
        }

        private boolean checkRecent(@Nonnull State state) {
            if (recent.getIfPresent(state) != null)
                return true; //was recently visited
            recent.put(state, true);
            return false; //not recent
        }

        private void advance(@Nonnull State state) {
            for (Op node : state.nodes) {
                g.forEachNeighbor(node, (i, neighbor) -> {
                    if (state.nodes.contains(neighbor))
                        return; // do not add something it already has
                    State next = state.tryAdvance(neighbor);
                    if (next != null) {
                        assert next.nodes.size() > state.nodes.size(); //invariant
                        globallyUnsatisfied.removeAll(neighbor.getMatchedTriples());
                        queue.add(next);
                    } else {
                        IndexSubset<Triple> m = full.fullSubset();
                        m.removeAll(state.matched);
                        if (!m.isEmpty() && (m.size() <= 4 || m.size() < full.size()/4))
                            missingSmall.add(m);
                    }
                });
            }
        }
    }

}
