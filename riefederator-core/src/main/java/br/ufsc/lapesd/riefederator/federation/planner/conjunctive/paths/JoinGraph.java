package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.paths;

import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.UndirectedIrreflexiveArrayGraph;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class JoinGraph extends UndirectedIrreflexiveArrayGraph<Op, JoinInfo> {
    public JoinGraph(@Nonnull RefIndexSet<Op> nodes) {
        super(JoinInfo.class, null, nodes);
    }

    public JoinGraph() {
        super(JoinInfo.class);
    }

    @Override
    public @Nonnull RefIndexSet<Op> getNodes() {
        return (RefIndexSet<Op>) super.getNodes();
    }

    @Override
    protected @Nullable JoinInfo weigh(@Nonnull Op l, @Nonnull Op r) {
        JoinInfo info = JoinInfo.getJoinability(l, r);
        return info.isValid() ? info : null;
    }
}
