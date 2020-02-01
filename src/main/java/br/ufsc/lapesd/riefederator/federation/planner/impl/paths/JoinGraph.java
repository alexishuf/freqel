package br.ufsc.lapesd.riefederator.federation.planner.impl.paths;

import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.util.UndirectedIrreflexiveArrayGraph;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;


public class JoinGraph extends UndirectedIrreflexiveArrayGraph<PlanNode, JoinInfo> {
    public JoinGraph(@Nonnull List<PlanNode> nodes) {
        super(JoinInfo.class, null, nodes);
    }

    @Override
    protected @Nullable JoinInfo weigh(@Nonnull PlanNode l, @Nonnull PlanNode r) {
        JoinInfo info = JoinInfo.getPlainJoinability(l, r);
        return info.isValid() ? info : null;
    }
}
