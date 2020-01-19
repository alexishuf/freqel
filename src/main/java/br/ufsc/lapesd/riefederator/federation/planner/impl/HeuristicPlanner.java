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
import static com.google.common.collect.Lists.cartesianProduct;
import static java.util.Collections.singletonList;

public class HeuristicPlanner implements Planner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull Collection<QueryNode> conjunctiveNodes) {
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

    static class JoinGraph extends UndirectedIrreflexiveArrayGraph<PlanNode> {
        private boolean withInputs;
        private @Nonnull Multimap<Set<Triple>, QueryNode> triples2qn;

        private static boolean hasOrHidesInputs(@Nonnull PlanNode node) {
            return node.hasInputs() || (node instanceof MultiQueryNode
                    && node.getChildren().stream().anyMatch(PlanNode::hasInputs));
        }

        @Override
        protected float weigh(@Nonnull PlanNode l, @Nonnull PlanNode r) {
            Set<Triple> lm = l.getMatchedTriples(), rm = r.getMatchedTriples();
            if (lm.containsAll(rm) || rm.containsAll(lm))
                return 0;
            if (l instanceof MultiQueryNode || r instanceof MultiQueryNode) {
                float sum = 0;
                int count  = 0;
                List<PlanNode> lNodes, rNodes;
                lNodes = l instanceof MultiQueryNode ? l.getChildren() : singletonList(l);
                rNodes = r instanceof MultiQueryNode ? r.getChildren() : singletonList(r);
                for (List<PlanNode> pair : cartesianProduct(lNodes, rNodes)) {
                    sum += weigh(pair.get(0), pair.get(1));
                    ++count;
                }
                return sum/count;
            } else {
                return joinVars(l, r).size();
            }
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
            super(new ArrayList<>(leaves));
            this.triples2qn = computeTriples2qn();
            this.withInputs = leaves.stream().anyMatch(JoinGraph::hasOrHidesInputs);
        }

        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull float[] intersection,
                         @Nullable Multimap<Set<Triple>, QueryNode> triples2qn,
                         boolean withInputs) {
            super(leaves, intersection);
            this.triples2qn = triples2qn == null ? computeTriples2qn() : triples2qn;
            this.withInputs = withInputs;
        }
        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull float[] intersection) {
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
            PlanNode root;
            if (withInputs) {
                JoinGraph other = buildTreeWithInputs(true);
                if (other == null)
                    return new EmptyNode(TreeUtils.unionResults(getNodes()));
                stealFrom(other);
                withInputs = other.withInputs;
                assert !isEmpty();
                root = get(0);
            } else {
                root = buildTreeNoInputs();
            }
            if (root == null || streamPreOrder(root).anyMatch(EmptyNode.class::isInstance))
                return new EmptyNode(TreeUtils.unionResults(getNodes()));
            return cleanMultiQuery(root);
        }

        private static @Nonnull PlanNode cleanMultiQuery(@Nonnull PlanNode node,
                                                         @Nonnull PlanNode other) {
            assert node instanceof MultiQueryNode;
            List<PlanNode> otherNodes = other instanceof MultiQueryNode ? other.getChildren()
                                                                        : singletonList(other);
            ArrayList<PlanNode> list = new ArrayList<>(node.getChildren());
            list.removeIf(child -> otherNodes.stream().noneMatch(
                    o -> o.getResultVars().containsAll(child.getInputVars())
                            && child.getInputVars().stream().noneMatch(o.getInputVars()::contains)
                    )
            );
            assert !list.isEmpty();
            if (list.size() == node.getChildren().size())
                return node;
            else if (list.size() == 1)
                return list.get(0);
            else
                return MultiQueryNode.builder().addAll(list).intersectInputs().build();
        }

        private static @Nonnull PlanNode cleanMultiQuery(@Nonnull PlanNode root) {
            Map<PlanNode, PlanNode> replace = new HashMap<>();
            for (PlanNode child : root.getChildren()) {
                PlanNode clean = cleanMultiQuery(child);
                if (clean != child)
                    replace.put(child, clean);
            }
            if (!replace.isEmpty())
                root = root.replacingChildren(replace);

            if (root instanceof JoinNode
                    && root.getChildren().stream().anyMatch(n -> n instanceof MultiQueryNode)) {
                PlanNode l = ((JoinNode) root).getLeft(), r = ((JoinNode) root).getRight();
                PlanNode l2 = l instanceof MultiQueryNode ? cleanMultiQuery(l, r) : l;
                PlanNode r2 = r instanceof MultiQueryNode ? cleanMultiQuery(r, l) : r;
                if (l2 != l || r2 != r) {
                    JoinNode replacement = JoinNode.builder(l2, r2).build();
                    assert replacement.getResultVars().equals(root.getResultVars());
                    root = replacement;
                }
            }
            return root;
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
                List<PlanNode> lList, rList;
                lList = l instanceof MultiQueryNode ? l.getChildren() : singletonList(l);
                rList = r instanceof MultiQueryNode ? r.getChildren() : singletonList(r);
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
            float[] weightsSubset = getWeightsForSubset(members, nodeSubset);
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
