package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;

import javax.annotation.Nonnull;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ArbitraryJoinOrderPlanner implements JoinOrderPlanner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull List<JoinInfo> joins) {
        checkArgument(!joins.isEmpty());
        JoinNode root = null;
        JoinInfo last = null;
        for (JoinInfo info : joins) {
            if (root == null) {
                root = JoinNode.builder(info.getLeft(), info.getRight()).build();
            } else {
                root = JoinNode.builder(root, info.getOppositeToLinked(last)).build();
            }
            last = info;
        }
        return root;
    }
}
