package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.joinVars;
import static com.google.common.base.Preconditions.checkArgument;

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
        return roots.size() == 1 ? roots.get(0) : new CartesianNode(roots);
    }

    static class JoinGraph {
        private ArrayList<PlanNode> leaves;
        private boolean withInputs;
        private int[][] intersection;

        private static int weight(@Nonnull PlanNode l, @Nonnull PlanNode r) {
            if (l instanceof QueryNode && r instanceof QueryNode) {
                QueryNode ln = (QueryNode)l, rn = (QueryNode)r;
                CQuery lq = ln.getQuery(), rq = rn.getQuery();
                if (lq.unorderedEquals(rq.getList()) && ln.getEndpoint().equals(rn.getEndpoint()))
                    return 0;
            }
            return joinVars(l, r).size();
        }

        public JoinGraph(@Nonnull Collection<? extends PlanNode> leaves) {
            this.leaves = new ArrayList<>(leaves);
            this.withInputs = leaves.stream().anyMatch(PlanNode::hasInputs);
            int size = this.leaves.size();

            intersection = new int[size][size];
            for (int i = 0; i < size; i++) {
                for (int j = i+1; j < size; j++)
                    intersection[i][j] = weight(this.leaves.get(i), this.leaves.get(j));
            }
        }

        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull int[][] intersection,
                         boolean withInputs) {
            this.leaves = leaves;
            this.withInputs = withInputs;
            this.intersection = intersection;
        }
        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull int[][] intersection) {
            this(leaves, intersection, leaves.stream().anyMatch(PlanNode::hasInputs));
        }

        private @Nonnull List<ImmutablePair<Integer, Integer>> sortByIntersection() {
            int size = leaves.size();
            int pairsCapacity = size * (size + 1) / 2;
            List<ImmutablePair<Integer, Integer>> pairs = new ArrayList<>(pairsCapacity);
            for (int i = 0; i < size; i++) {
                for (int j = i+1; j < size; j++) pairs.add(ImmutablePair.of(i, j));
            }
            Comparator<ImmutablePair<Integer, Integer>> comparator
                    = Comparator.comparing(p -> intersection[p.left][p.right]);
            pairs.sort(comparator.reversed());
            return pairs;
        }

        public @Nonnull PlanNode buildTree() {
            if (withInputs) {
                JoinGraph other = buildTreeWithInputs(true);
                if (other == null)
                    return new EmptyNode(TreeUtils.unionResults(leaves));
                leaves = other.leaves;
                intersection = other.intersection;
                withInputs = other.withInputs;
                assert !leaves.isEmpty();
                return leaves.get(0);
            }
            PlanNode root = buildTreeNoInputs();
            return root == null ? new EmptyNode(TreeUtils.unionResults(leaves)) : root;
        }

        public @Nullable JoinGraph buildTreeWithInputs(boolean first) {
            Preconditions.checkState(!leaves.isEmpty(), "Can't build a tree without leaves!");
            if (leaves.size() == 1) {
                PlanNode root = leaves.get(0);
                if (root.hasInputs())
                    return null;
                return this; //ready
            }

            List<ImmutablePair<Integer, Integer>> pairs = sortByIntersection();
            for (ImmutablePair<Integer, Integer> pair : pairs) {
                if (intersection[pair.left][pair.right] == 0)
                    return null; //pairs are ordered, no more valid alternatives
                PlanNode l = leaves.get(pair.left), r = leaves.get(pair.right);
                if (first && l.hasInputs() && r.hasInputs())
                    continue; //left-most join must not have inputs
                JoinGraph root = buildTreeWithInputs(pair);
                if (root != null)
                    return root; //found a join and applied it
            }
            return null; // no join is possible
        }

        @SuppressWarnings("ManualArrayCopy")
        public @Nullable JoinGraph
        buildTreeWithInputs(@Nonnull ImmutablePair<Integer, Integer> pair) {
            PlanNode left = leaves.get(pair.left), right = leaves.get(pair.right);
            JoinNode joinNode = JoinNode.builder(left, right).build();

            boolean stillWithInputs = false;
            int idx = -1;
            for (PlanNode leaf : leaves) {
                ++idx;
                if (idx != pair.left && idx != pair.right && leaf.hasInputs()) {
                    stillWithInputs = true;
                    break;
                }
            }

            ArrayList<PlanNode> leavesCp = new ArrayList<>(leaves);
            int size = leaves.size();
            int[][] intersectionCp = new int[size][size];
            for (int i = 0; i < size; i++) {
                for (int j = i+1; j < size; j++)
                    intersectionCp[i][j] = intersection[i][j];
            }
            JoinGraph other = new JoinGraph(leavesCp, intersectionCp, stillWithInputs);
            other.replaceWithJoin(pair.left, pair.right, joinNode);

            return other.buildTreeWithInputs(false);
        }

        public @Nullable PlanNode buildTreeNoInputs() {
            while (true) {
                if (!tryJoin()) break;
            }
            Preconditions.checkState(leaves.size() == 1);
            return leaves.get(0);
        }

        public boolean tryJoin() {
            if (leaves.size() < 2)
                return false; //nothing to join
            int size = leaves.size();
            // find maximal intersection
            int max = 0, maxI = -1, maxJ = -1;
            for (int i = 0; i < size; i++) {
                for (int j = i+1; j < size; j++) {
                    if (intersection[i][j] > max)
                        max = intersection[maxI = i][maxJ = j];
                }
            }
            if (maxI < 0) // disjoint graph
                return false;
            assert maxI != maxJ;

            JoinNode joinNode = JoinNode.builder(leaves.get(maxI), leaves.get(maxJ)).build();
            replaceWithJoin(maxI, maxJ, joinNode);
            return true;
        }

        @SuppressWarnings("ManualArrayCopy") // System.arraycopy() is almost unreadable here
        protected void replaceWithJoin(int leftIdx, int rightIdx, @Nonnull JoinNode joinNode) {
            assert leftIdx < rightIdx;
            int size = leaves.size();
            leaves.set(leftIdx, joinNode);
            leaves.remove(rightIdx);

            // adjust intersection to the changes
            for (int i = 0; i < leftIdx; i++) {
                // re-compute intersection, now with the JoinNode
                intersection[i][leftIdx] = weight(leaves.get(i), joinNode);
                // shift down unaffected intersections
                for (int j = rightIdx; j < size - 1; j++)
                    intersection[i][j] = intersection[i][j + 1];
            }
            // re-compute intersection, now with the JoinNode
            for (int j = leftIdx; j < size - 1; j++)
                intersection[leftIdx][j] = weight(joinNode, leaves.get(j));
            for (int i = leftIdx +1; i < size-1; i++) {
                // shift down unaffected intersections
                for (int j = i+1; j < size - 1; j++)
                    intersection[i][j] = intersection[i][j + 1];
            }
        }

        public void forEachConnectedComponent(@Nonnull Consumer<JoinGraph> consumer) {
            int size = leaves.size();
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
                    for (int i = 0; i < idx; i++) {
                        if ( intersection[i][idx] > 0) queue.add(i);
                    }
                    for (int i = idx+1; i < size; i++) {
                        if ( intersection[idx][i] > 0) queue.add(i);
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
        @Nonnull ArrayList<PlanNode> getLeaves() {
            return leaves;
        }

        @VisibleForTesting
        @Nonnull int[][] getIntersection() {
            return intersection;
        }

        @VisibleForTesting
        @Nonnull JoinGraph createComponent(BitSet members) {
            int memberCount = members.cardinality();
            ArrayList<PlanNode> subset = new ArrayList<>(memberCount);
            int[][] intersection = new int[memberCount][memberCount];
            int totalSize = this.leaves.size();
            int outI = 0;
            for (int i = 0; i < totalSize; i++) {
                if (!members.get(i))
                    continue;
                subset.add(this.leaves.get(i));
                int outJ = outI+1;
                for (int j = i+1; j < totalSize; j++) {
                    if (members.get(j))
                        intersection[outI][outJ++] = this.intersection[i][j];
                }
                ++outI;
            }
            return new JoinGraph(subset, intersection);
        }
    }

    private List<PlanNode> groupSameQueries(@Nonnull Collection<QueryNode> leafs) {
        Multimap<CQuery, QueryNode> q2qn = MultimapBuilder.hashKeys(leafs.size())
                                                          .arrayListValues().build();
        for (QueryNode qn : leafs)
            q2qn.put(qn.getQuery(), qn);
        List<PlanNode> grouped = new ArrayList<>(q2qn.keySet().size());
        for (CQuery query : q2qn.keySet()) {
            Collection<QueryNode> values = q2qn.get(query);
            assert !values.isEmpty();
            if (values.size() > 1)
                grouped.add(MultiQueryNode.builder().addAll(values).build());
            else
                grouped.add(values.iterator().next());
        }
        return grouped;
    }
}
