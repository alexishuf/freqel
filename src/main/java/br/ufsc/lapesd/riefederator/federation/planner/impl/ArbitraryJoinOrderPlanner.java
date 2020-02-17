package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import java.util.Collection;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.cleanEquivalents;
import static com.google.common.base.Preconditions.checkArgument;

public class ArbitraryJoinOrderPlanner implements JoinOrderPlanner {

    @Override
    public @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph,
                                  @Nonnull Collection<PlanNode> nodesCollection) {
        checkArgument(!nodesCollection.isEmpty(), "Cannot plan joins without any nodes!");
        IndexedSet<PlanNode> nodes = IndexedSet.from(nodesCollection);
        IndexedSubset<PlanNode> pending = nodes.fullSubset();
        PlanNode root = pending.iterator().next();
        pending.remove(root);
        root = cleanEquivalents(root);
        while (!pending.isEmpty()) {
            PlanNode selected = null;
            for (PlanNode candidate : pending) {
                if (JoinInfo.getPlainJoinability(root, candidate).isValid()) {
                    selected = candidate;
                    break;
                }
            }
            if (selected == null)
                throw new IllegalArgumentException("nodesCollection is not join-connected");
            pending.remove(selected);
            root = JoinNode.builder(root, cleanEquivalents(selected)).build();
        }
        return root;
    }

//    @Override
//    public @Nonnull PlanNode plan(@Nonnull List<JoinInfo> joins, @Nullable JoinGraph ignored) {
//        checkArgument(!joins.isEmpty());
//        JoinNode root = null;
//        JoinInfo last = null;
//        for (JoinInfo info : joins) {
//            if (root == null) {
//                PlanNode l = cleanEquivalents(info.getLeft()), r = cleanEquivalents(info.getRight());
//                root = JoinNode.builder(l, r).build();
//            } else {
//                PlanNode clean = cleanEquivalents(info.getOppositeToLinked(last));
//                root = JoinNode.builder(root, clean).build();
//            }
//            last = info;
//        }
//        return root;
//    }

}
