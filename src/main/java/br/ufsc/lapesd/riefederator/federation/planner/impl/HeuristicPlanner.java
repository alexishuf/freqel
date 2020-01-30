package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;
import br.ufsc.lapesd.riefederator.util.UndirectedIrreflexiveArrayGraph;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.*;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class HeuristicPlanner implements Planner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull CQuery ignored,
                                  @Nonnull Collection<QueryNode> conjunctiveNodes) {
        checkArgument(!conjunctiveNodes.isEmpty(), "Cannot plan empty queries");
        if (conjunctiveNodes.size() == 1)
            return conjunctiveNodes.iterator().next();
        List<PlanNode> leaves = groupSameQueries(conjunctiveNodes);
        JoinGraph firstGraph = new JoinGraph(leaves);
        List<PlanNode> roots = new ArrayList<>();
        firstGraph.forEachConnectedComponent(component -> roots.add(component.buildTree()));
        assert !roots.isEmpty();
        assert roots.stream().noneMatch(Objects::isNull);
        if (roots.stream().anyMatch(EmptyNode.class::isInstance))
            return new EmptyNode(unionResults(leaves));
        return roots.size() == 1 ? roots.get(0) : new CartesianNode(roots);
    }

    static class JoinGraph extends UndirectedIrreflexiveArrayGraph<PlanNode, Float> {
        private boolean withInputs;
        private @Nonnull Multimap<Set<Triple>, QueryNode> triples2qn;

        private static boolean hasOrHidesInputs(@Nonnull PlanNode node) {
            return node.hasInputs() || (node instanceof MultiQueryNode
                    && node.getChildren().stream().anyMatch(PlanNode::hasInputs));
        }

        @Override
        protected Float weigh(@Nonnull PlanNode l, @Nonnull PlanNode r) {
            JoinInfo join = JoinInfo.getMultiJoinability(l, r);
            if (!join.isValid()) return 0.0f;
            int count = join.getLeftNodes().size() * join.getRightNodes().size();
            float sum = 0;
            for (JoinInfo value : join.getChildJoins().values())
                sum += value.getJoinVars().size();
            return sum / count;
        }

        private Multimap<Set<Triple>, QueryNode> computeTriples2qn() {
            Multimap<Set<Triple>, QueryNode> map;
            map = MultimapBuilder.hashKeys().arrayListValues().build();
            for (PlanNode leaf : getNodes()) {
                if (leaf instanceof MultiQueryNode) {
                    for (PlanNode member : leaf.getChildren()) {
                        if (member instanceof QueryNode)
                            map.put(((QueryNode)member).getQuery().getSet(), (QueryNode)member);
                    }
                } else if (leaf instanceof QueryNode) {
                    map.put(((QueryNode)leaf).getQuery().getSet(), (QueryNode)leaf);
                }
            }
            return map;
        }

        public JoinGraph(@Nonnull Collection<? extends PlanNode> leaves) {
            super(Float.class, 0.0f, new ArrayList<>(leaves));
            this.triples2qn = computeTriples2qn();
            this.withInputs = leaves.stream().anyMatch(JoinGraph::hasOrHidesInputs);
        }

        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull Float[] intersection,
                         @Nullable Multimap<Set<Triple>, QueryNode> triples2qn,
                         boolean withInputs) {
            super(Float.class, 0.0f, leaves, intersection);
            this.triples2qn = triples2qn == null ? computeTriples2qn() : triples2qn;
            this.withInputs = withInputs;
        }
        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull Float[] intersection) {
            this(leaves, intersection, null,
                    leaves.stream().anyMatch(JoinGraph::hasOrHidesInputs));
        }

        private void updateWithInputs() {
            withInputs = getNodes().stream().anyMatch(JoinGraph::hasOrHidesInputs);
        }

        private @Nonnull List<ImmutablePair<Integer, Integer>> sortByIntersection() {
            int size = size();
            int pairsCapacity = size * (size + 1) / 2;
            List<ImmutablePair<Integer, Integer>> pairs = new ArrayList<>(pairsCapacity);
            for (int i = 0; i < size; i++) {
                for (int j = i+1; j < size; j++) pairs.add(ImmutablePair.of(i, j));
            }
            Comparator<ImmutablePair<Integer, Integer>> comparator
                    = Comparator.comparing(p -> getWeight(p.left, p.right));
            pairs.sort(comparator.reversed());
            return pairs;
        }

        public @Nonnull PlanNode buildTree() {
            Set<QueryNode> selected = new HashSet<>();
            return buildTree(selected);
        }

        private  @Nonnull PlanNode buildTree(@Nonnull Set<QueryNode> selected) {
            PlanNode root;
            if (withInputs) {

                JoinGraph other = buildTreeWithInputs(true);
                if (other == null)
                    return new EmptyNode(unionResults(getNodes()));
                Set<Triple> allTriples = allMatchedTriples(getNodes());
                stealFrom(other);
                withInputs = other.withInputs;
                assert !isEmpty();
                PlanNode dirtyRoot = get(0);
                if (!dirtyRoot.getMatchedTriples().containsAll(allTriples))
                    return new EmptyNode(unionResults(getNodes()));
                List<QueryNode> discarded = new ArrayList<>();
                if (getNodes().size() > 1) {
                    getNodes().listIterator(1)
                            .forEachRemaining(n -> addAllQueryNodes(discarded, n));
                }
                root = decayToEmpty(cleanMQ(dirtyRoot, emptySet(), discarded));
                if (!(root instanceof EmptyNode) && !discarded.isEmpty()) {
                    addAllQueryNodes(selected, root);
                    PlanNode alternative = buildTreeWithDiscarded(selected, discarded, allTriples);
                    if (alternative != null) {
                        assert streamPreOrder(alternative).noneMatch(EmptyNode.class::isInstance);
                        root = MultiQueryNode.builder().addAll(root).addAll(alternative).build();
                    }
                }
            } else {
                root = decayToEmpty(buildTreeNoInputs());
            }
            return root;
        }

        private static @Nonnull Set<Triple>
        allMatchedTriples(@Nonnull Collection<? extends PlanNode> nodes) {
            return nodes.stream().flatMap(n->n.getMatchedTriples().stream()).collect(toSet());
        }

        private @Nonnull PlanNode decayToEmpty(@Nullable PlanNode root) {
            if (root instanceof MultiQueryNode) {
                MultiQueryNode.Builder builder = MultiQueryNode.builder();
                boolean allIn = true;
                for (PlanNode child : root.getChildren()) {
                    if (streamPreOrder(child).noneMatch(EmptyNode.class::isInstance)) {
                        allIn = false;
                        builder.add(child);
                    }
                }
                if (allIn)
                    return root;
                else if (!builder.isEmpty())
                    return builder.buildIfMulti();
            } else if (root != null) {
                if (streamPreOrder(root).noneMatch(EmptyNode.class::isInstance))
                    return root;
            }
            return new EmptyNode(unionResults(getNodes()));
        }

        private @Nullable PlanNode buildTreeWithDiscarded(@Nonnull Set<QueryNode> selected,
                                                          @Nonnull List<QueryNode> discarded,
                                                          @Nonnull Set<Triple> allTriples) {
            ArrayList<QueryNode> all = new ArrayList<>(selected.size()+discarded.size());
            all.addAll(discarded);
            for (QueryNode sNode : selected) {
                if (discarded.stream().noneMatch(dNode -> shouldDiscard(dNode, sNode, discarded)))
                    all.add(sNode);
            }
            if (!allMatchedTriples(all).containsAll(allTriples))
                return null;
            List<PlanNode> grouped = groupSameQueries(all);
            JoinGraph g = new JoinGraph(grouped);
            MultiQueryNode.Builder builder = MultiQueryNode.builder();
            g.forEachConnectedComponent(c -> builder.add(c.buildTree(selected)));
            PlanNode root = builder.buildIfMulti();
            if (childrenIfMulti(root).stream().anyMatch(EmptyNode.class::isInstance))
                return null;
            return root;
        }

        private static boolean shouldDiscard(@Nonnull QueryNode disc, @Nonnull QueryNode sel,
                                             @Nonnull List<QueryNode> discNodes) {
            if (!disc.getMatchedTriples().containsAll(sel.getMatchedTriples())) {
                // disc does not subsume sel. Only discard sel if some other disc has the extra
                Set<Triple> extra = setMinus(sel.getMatchedTriples(), disc.getMatchedTriples());
                return discNodes.stream().anyMatch(d -> d.getMatchedTriples().containsAll(extra));
            }
            // if sel has a different set of inputs, it may cause elimination of disc later
            return !disc.getInputVars().equals(sel.getInputVars());
        }

        private static @Nullable PlanNode cleanMQ(@Nonnull PlanNode root,
                                                  @Nonnull Set<String> allowedInputs,
                                                  @Nonnull List<QueryNode> outRemoved) {
            if (root instanceof JoinNode) {
                PlanNode l = ((JoinNode) root).getLeft(), r = ((JoinNode) root).getRight();
                PlanNode l2, r2;
                boolean lMQ = l instanceof MultiQueryNode, rMQ = r instanceof MultiQueryNode;
                if (lMQ || rMQ) {
                    List<PlanNode> left = new ArrayList<>(childrenIfMulti(l));
                    List<PlanNode> right = new ArrayList<>(childrenIfMulti(r));
                    removeNotJoinable(left, right, allowedInputs, outRemoved);
                    if (left.isEmpty() || right.isEmpty())
                        return null;
                    l2 = lMQ ? ((MultiQueryNode)l).with(left)  : left.get(0);
                    r2 = rMQ ? ((MultiQueryNode)r).with(right) : right.get(0);
                } else {
                    if (HeuristicPlanner.class.desiredAssertionStatus()) {
                        Set<String> pending = new HashSet<>();
                        Set<String> vars = joinVars(l, r, pending);
                        assert !vars.isEmpty() && allowedInputs.containsAll(pending);
                    }
                    HashSet<String> subAllowedInputs = new HashSet<>(allowedInputs);
                    subAllowedInputs.addAll(setMinus(r.getResultVars(), r.getInputVars()));
                    l2 = cleanMQ(l, subAllowedInputs, outRemoved);

                    subAllowedInputs = new HashSet<>(allowedInputs);
                    subAllowedInputs.addAll(setMinus(l.getResultVars(), l.getInputVars()));
                    r2 = cleanMQ(r, subAllowedInputs, outRemoved);
                }
                if (l2 == null || r2 == null)
                    return null;
                else if (l2 != l || r2 != r)
                    return JoinNode.builder(l2, r2).build(); //no need to remove alternatives
                else
                    return root;
            } else if (root instanceof MultiQueryNode) {
                Map<PlanNode, PlanNode> replacements = new HashMap<>();
                List<PlanNode> removals = new ArrayList<>();
                for (PlanNode child : root.getChildren()) {
                    PlanNode replacement = cleanMQ(child, allowedInputs, outRemoved);
                    if (replacement == null)
                        removals.add(child);
                    else if (replacement != child)
                        replacements.put(child, replacement);
                }
                if (!replacements.isEmpty())
                    root = root.replacingChildren(replacements);
                if (!removals.isEmpty()) {
                    removals.forEach(r -> addAllQueryNodes(outRemoved, r));
                    root = ((MultiQueryNode) root).without(removals);
                }
                return root;
            } else {
                assert root instanceof QueryNode;
                return root;
            }
        }

        private static class BestJoinState {
            PlanNode left, right = null;
            PlanNode cleanLeft = null, cleanRight = null;
            List<QueryNode> minRemovals = null;
            Set<String> allowedInputs, pending = new HashSet<>();
            List<QueryNode> subRemovals = new ArrayList<>();

            public BestJoinState(@Nonnull Set<String> allowedInputs) {
                this.allowedInputs = allowedInputs;
            }

            public void setLeft(@Nonnull PlanNode left) {
                this.left = left;
                cleanLeft = cleanRight = null;
                minRemovals = null;

            }

            public void offerRight(@Nonnull PlanNode right) {
                if (cannotJoin(left, right)) return;

                subRemovals.clear();
                PlanNode left2  = cleanForJoin(left, right);
                if (left2 == null) return;
                PlanNode right2 = cleanForJoin(right, left);
                if (right2 == null) return;

                if (cannotJoin(left2, right2)) return;

                if (minRemovals == null || subRemovals.size() < minRemovals.size()) {
                    cleanLeft = left2;
                    cleanRight = right2;
                    this.right = right;
                    minRemovals = new ArrayList<>(subRemovals);
                }
            }

            private boolean cannotJoin(@Nonnull PlanNode l, @Nonnull PlanNode r) {
                pending.clear();
                return joinVars(l, r, pending).isEmpty() || !allowedInputs.containsAll(pending);
            }

            private @Nullable PlanNode cleanForJoin(@Nonnull PlanNode l, @Nonnull PlanNode r) {
                Set<String> subAllowed = new HashSet<>(allowedInputs);
                subAllowed.addAll(setMinus(r.getResultVars(), r.getInputVars()));
                return cleanMQ(l, subAllowed, subRemovals);
            }
        }

        private static void addAllQueryNodes(@Nonnull Collection<QueryNode> c, @Nonnull PlanNode r){
            streamPreOrder(r).filter(QueryNode.class::isInstance)
                    .map(n -> (QueryNode)n).forEach(c::add);
        }

        private static void removeNotJoinable(@Nonnull List<PlanNode> outer,
                                              @Nonnull List<PlanNode> inner,
                                              @Nonnull Set<String> allowedInputs,
                                              @Nonnull List<QueryNode> outRemoved) {
            BestJoinState bestJoinState = new BestJoinState(allowedInputs);
            Set<PlanNode> closedInner = new HashSet<>();
            removeNotJoinable(outer, inner, bestJoinState, outRemoved, emptySet(), closedInner);
            removeNotJoinable(inner, outer, bestJoinState, outRemoved, closedInner, null);
        }

        private static void removeNotJoinable(@Nonnull List<PlanNode> outer,
                                              @Nonnull List<PlanNode> inner,
                                              @Nonnull BestJoinState bestJoinState,
                                              @Nonnull List<QueryNode> outRemoved,
                                              @Nonnull Set<PlanNode> inClosed,
                                              @Nullable Set<PlanNode> outClosed) {
            for (int i = 0; i < outer.size(); i++) {
                PlanNode l = outer.get(i);
                if (inClosed.contains(l)) continue;
                bestJoinState.setLeft(l);
                inner.forEach(bestJoinState::offerRight);
                if (bestJoinState.cleanLeft == null) {
                    outer.remove(i--);
                    addAllQueryNodes(outRemoved, l);
                } else {
                    outRemoved.addAll(bestJoinState.minRemovals);
                    outer.set(i, bestJoinState.cleanLeft);
                    int idx = inner.indexOf(bestJoinState.right);
                    assert idx >=0;
                    inner.set(idx, bestJoinState.cleanRight);
                    if (outClosed != null)
                        outClosed.add(bestJoinState.cleanRight);
                }
            }
        }

        public @Nullable JoinGraph buildTreeWithInputs(boolean first) {
            Preconditions.checkState(!isEmpty(), "Can't build a tree without leaves!");
            if (size() == 1) {
                PlanNode root = get(0);
                if (root.hasInputs())
                    return null;
                return this; //ready
            }
            if (!withInputs)
                return buildTreeNoInputs() == null ? null : this;

            List<ImmutablePair<Integer, Integer>> pairs = sortByIntersection();
            for (ImmutablePair<Integer, Integer> pair : pairs) {
                if (getWeight(pair.left, pair.right) == 0)
                    return null; //pairs are ordered, no more valid alternatives
                PlanNode l = get(pair.left), r = get(pair.right);
                if (first && l.hasInputs() && r.hasInputs())
                    continue; //left-most join must not have inputs
                JoinGraph root = buildTreeWithInputs(pair);
                if (root != null)
                    return root; //found a join and applied it
            }
            return null; // no join is possible
        }

        public @Nullable JoinGraph
        buildTreeWithInputs(@Nonnull ImmutablePair<Integer, Integer> pair) {
            JoinGraph other = new JoinGraph(new ArrayList<>(getNodes()),
                                            getWeights(), triples2qn, withInputs);
            other.replaceWithJoin(pair.left, pair.right);
            other.updateWithInputs();

            return other.buildTreeWithInputs(false);
        }

        public @Nullable PlanNode buildTreeNoInputs() {
            while (true) {
                if (!tryJoin()) break;
            }
            Preconditions.checkState(size() == 1);
            return get(0);
        }

        public boolean tryJoin() {
            if (size() < 2)
                return false; //nothing to join
            int size = size();
            // find maximal intersection
            int maxI = -1, maxJ = -1;
            float max = 0;
            for (int i = 0; i < size; i++) {
                for (int j = i+1; j < size; j++) {
                    float weight = getWeight(i, j);
                    if (weight > max) {
                        max = weight;
                        maxI = i;
                        maxJ = j;
                    }
                }
            }
            if (maxI < 0) // disjoint graph
                return false;
            assert maxI != maxJ;
            replaceWithJoin(maxI, maxJ);
            return true;
        }

        protected void replaceWithJoin(int leftIdx, int rightIdx) {
            JoinNode joinNode = createJoinNode(leftIdx, rightIdx);
            removeAt(rightIdx);
            replaceAt(leftIdx, joinNode);
        }

        @VisibleForTesting
        @Nonnull JoinNode createJoinNode(int leftIdx, int rightIdx) {
            PlanNode l = get(leftIdx), r = get(rightIdx);
            //noinspection AssertWithSideEffects /* there are no side effects */
            assert !(l instanceof QueryNode && r instanceof QueryNode &&
                    ((QueryNode)l).getEndpoint().isAlternative(((QueryNode)r).getEndpoint()) &&
                    ((QueryNode)l).getQuery().getSet().equals(((QueryNode)r).getQuery().getSet()))
                    : "left and right have the same query and alternative endpoints. " +
                      "They should never be joined";

            if (l instanceof MultiQueryNode || r instanceof MultiQueryNode) {
                List<PlanNode> lList = childrenIfMulti(l), rList = childrenIfMulti(r);
                List<PlanNode> lOk = new ArrayList<>(lList.size());
                List<PlanNode> rOk = new ArrayList<>(rList.size());
                for (PlanNode lMember : lList) {
                    for (PlanNode rMember : rList) {
                        if (!joinVars(lMember, rMember).isEmpty()) {
                            lOk.add(lMember);
                            rOk.add(rMember);
                            break;
                        }
                    }
                }
                for (PlanNode rMember : rList) {
                    if (rOk.contains(rMember)) continue;
                    for (PlanNode lMember : lList) {
                        if (!joinVars(lMember, rMember).isEmpty()) {
                            rOk.add(rMember);
                            break;
                        }
                    }
                }
                removeAlternativesFromList(lOk);
                removeAlternativesFromList(rOk);
                assert !lOk.isEmpty() && !rOk.isEmpty();

                l = MultiQueryNode.builder().addAll(lOk).intersectInputs().buildIfMulti();
                r = MultiQueryNode.builder().addAll(rOk).intersectInputs().buildIfMulti();
                replaceAt(leftIdx, l);
                replaceAt(rightIdx, r);
                lOk.forEach(this::removeAlternatives);
                rOk.forEach(this::removeAlternatives);
            } else {
                removeAlternatives(l);
                removeAlternatives(r);
            }

            return JoinNode.builder(l, r).build();
        }

        void removeAlternativesFromList(@Nonnull List<PlanNode> list) {
            for (int i = 0; i < list.size()-1; i++) {
                PlanNode node = list.get(i);
                if (!(node instanceof QueryNode))
                    continue;
                TPEndpoint ep = ((QueryNode) node).getEndpoint();
                Set<Triple> triples = ((QueryNode) node).getQuery().getSet();
                ListIterator<PlanNode> it = list.listIterator(i + 1);
                while (it.hasNext()) {
                    PlanNode candidate = it.next();
                    if (!(candidate instanceof QueryNode))
                        continue;
                    QueryNode qn = (QueryNode)candidate;
                    Set<Triple> set = qn.getQuery().getSet();
                    if (set.equals(triples) && ep.isAlternative(qn.getEndpoint()))
                        it.remove();
                }
            }
        }

        void removeAlternatives(@Nonnull PlanNode node) {
            if (node instanceof MultiQueryNode) {
                node.getChildren().forEach(this::removeAlternatives);
            } else if (node instanceof QueryNode) {
                QueryNode qn = (QueryNode) node;
                for (QueryNode cand : triples2qn.get(qn.getQuery().getSet())) {
                    if (qn != cand && qn.getEndpoint().isAlternative(cand.getEndpoint())) {
                        int idx = 0;
                        for (PlanNode leaf : getNodes()) {
                            if (leaf == cand || leaf.getChildren().contains(cand))
                                break;
                            ++idx;
                        }
                        if (idx < size())
                            removeLeafAt(cand, idx);
                    }
                }
            }
        }

        @VisibleForTesting
        void removeLeafAt(@Nonnull QueryNode node, int idx) {
            PlanNode leaf = get(idx);
            assert leaf == node || leaf.getChildren().contains(node);

            if (leaf instanceof MultiQueryNode) {
                assert leaf.getChildren().contains(node);
                PlanNode rep = ((MultiQueryNode) leaf).without(node);
                if (rep != null) {
                    replaceAt(idx, rep);
                    return;
                } // else: rep would be empty, we need to remove leaf!
            }
            removeAt(idx);
        }

        public void forEachConnectedComponent(@Nonnull Consumer<JoinGraph> consumer) {
            int size = size();
            BitSet visited = new BitSet(size);
            BitSet component = new BitSet(size);
            Queue<Integer> queue = new ArrayDeque<>();
            queue.add(0);
            while (!queue.isEmpty()) {
                while (!queue.isEmpty()) {
                    int idx = queue.remove();
                    if (visited.get(idx)) continue;
                    visited.set(idx);
                    component.set(idx);
                    for (int i = 0; i < size; i++) {
                        if (i != idx && getWeight(idx, i) > 0) queue.add(i);
                    }
                }
                consumer.accept(createComponent(component));
                component.clear();
                for (int i = 0; i < size; i++) {
                    if (!visited.get(i)) {
                        queue.add(i);
                        break;
                    }
                }
            }
        }

        @VisibleForTesting
        @Nonnull JoinGraph createComponent(BitSet members) {
            ArrayList<PlanNode> nodeSubset = new ArrayList<>(members.cardinality());
            Float[] weightsSubset = getWeightsForSubset(members, nodeSubset);
            return new JoinGraph(nodeSubset, weightsSubset);
        }
    }

    @VisibleForTesting
    static List<PlanNode> groupSameQueries(@Nonnull Collection<QueryNode> leafs) {
        Multimap<Set<Triple>, QueryNode> q2qn = MultimapBuilder.hashKeys(leafs.size())
                                                          .arrayListValues().build();
        for (QueryNode qn : leafs)
            q2qn.put(qn.getMatchedTriples(), qn);
        List<PlanNode> grouped = new ArrayList<>(q2qn.keySet().size());
        for (Set<Triple> tripleSet : q2qn.keySet()) {
            Collection<QueryNode> values = withoutEquivalents(q2qn.get(tripleSet));
            assert !values.isEmpty();
            if (values.size() > 1)
                grouped.add(MultiQueryNode.builder().addAll(values).intersectInputs().build());
            else
                grouped.add(values.iterator().next());
        }
        return grouped;
    }

    @VisibleForTesting
    static @Nonnull List<QueryNode> withoutEquivalents(@Nonnull Collection<QueryNode> nodes) {
        Multimap<CQuery, QueryNode> mm = MultimapBuilder.hashKeys().arrayListValues().build();
        for (QueryNode node : nodes)
            mm.put(node.getQuery(), node);
        List<TPEndpoint> endpoints = new ArrayList<>(nodes.size());
        List<QueryNode> filtered = new ArrayList<>(nodes.size());
        for (CQuery query : mm.keySet()) {
            for (QueryNode queryNode : mm.get(query)) {
                TPEndpoint ep = queryNode.getEndpoint();
                if (endpoints.stream().noneMatch(ep::isAlternative)) {
                    endpoints.add(ep);
                    filtered.add(queryNode);
                }
            }
            endpoints.clear();
        }
        return filtered;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("HeuristicPlanner@%x", System.identityHashCode(this));
    }
}
