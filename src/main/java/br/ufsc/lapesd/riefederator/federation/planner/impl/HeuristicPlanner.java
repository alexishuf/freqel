package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Consumer;

public class HeuristicPlanner implements Planner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull Collection<QueryNode> conjunctiveNodes) {
        Preconditions.checkArgument(!conjunctiveNodes.isEmpty(), "Cannot plan empty queries");
        if (conjunctiveNodes.size() == 1)
            return conjunctiveNodes.iterator().next();
        List<PlanNode> leaves = groupSameQueries(conjunctiveNodes);
        JoinGraph firstGraph = new JoinGraph(leaves);
        List<PlanNode> roots = new ArrayList<>();
        firstGraph.forEachConnectedComponent(component -> {
            while (true) {
                if (!component.tryJoin()) break;
            }
            roots.add(component.getRoot());
        });
        assert !roots.isEmpty();
        return roots.size() == 1 ? roots.get(0) : new CartesianNode(roots);
    }

    static class JoinGraph {
        private ArrayList<PlanNode> leaves;
        private int[][] intersection;

        public JoinGraph(@Nonnull Collection<PlanNode> leaves) {
            this.leaves = new ArrayList<>(leaves);
            weighIntersections();
        }

        public JoinGraph(@Nonnull ArrayList<PlanNode> leaves,
                         @Nonnull int[][] intersection) {
            this.leaves = leaves;
            this.intersection = intersection;
        }

        @SuppressWarnings("ManualArrayCopy") // System.arraycopy() is almost unreadable here
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

            // replace maxI-th node with JoinNode and remove maxJ-th node
            JoinNode join = JoinNode.builder(leaves.get(maxI), leaves.get(maxJ)).build();
            leaves.set(maxI, join);
            leaves.remove(maxJ);

            // adjust intersection to the changes
            Set<String> temp = new HashSet<>(join.getResultVars().size());
            for (int i = 0; i < maxI; i++) {
                // re-compute intersection, now with the JoinNode
                temp.addAll(leaves.get(i).getResultVars());
                temp.retainAll(leaves.get(maxI).getResultVars());
                intersection[i][maxI] = temp.size();
                temp.clear();
                // shift down unaffected intersections
                for (int j = maxJ; j < size - 1; j++)
                    intersection[i][j] = intersection[i][j + 1];
            }
            for (int j = maxI; j < size - 1; j++) {
                // re-compute intersection, now with the JoinNode
                temp.addAll(leaves.get(maxI).getResultVars());
                temp.retainAll(leaves.get(j).getResultVars());
                intersection[maxI][j] = temp.size();
                temp.clear();
            }
            for (int i = maxI+1; i < size-1; i++) {
                // shift down unaffected intersections
                for (int j = i+1; j < size - 1; j++)
                    intersection[i][j] = intersection[i][j + 1];
            }
            return true;
        }

        public @Nonnull PlanNode getRoot() {
            Preconditions.checkState(leaves.size() == 1);
            return leaves.get(0);
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
                    for (int i = 0; i < idx-1; i++) {
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
            ArrayList<PlanNode> leafs = new ArrayList<>(memberCount);
            int[][] intersection = new int[memberCount][memberCount];
            int totalSize = this.leaves.size();
            int outI = 0;
            for (int i = 0; i < totalSize; i++) {
                if (!members.get(i))
                    continue;
                leafs.add(this.leaves.get(i));
                int outJ = outI+1;
                for (int j = i+1; j < totalSize; j++) {
                    if (members.get(j))
                        intersection[outI][outJ++] = this.intersection[i][j];
                }
                ++outI;
            }
            return new JoinGraph(leafs, intersection);
        }

        private void weighIntersections() {
            int size = leaves.size();
            intersection = new int[size][size];
            HashSet<String> temp = new HashSet<>();
            for (int i = 0; i < size; i++) {
                Set<String> left = leaves.get(i).getResultVars();
                for (int j = i+1; j < size; j++) {
                    temp.clear();
                    temp.addAll(left);
                    temp.retainAll(leaves.get(j).getResultVars());
                    intersection[i][j] = temp.size();
                }
            }
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
