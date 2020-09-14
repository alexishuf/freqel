package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.util.RefSet;
import com.google.inject.ImplementedBy;

import javax.annotation.Nonnull;

@ImplementedBy(DefaultFilterJoinPlanner.class)
public interface FilterJoinPlanner {
    @Nonnull Op rewrite(@Nonnull CartesianOp op, @Nonnull RefSet<Op> shared);
    @Nonnull Op rewrite(@Nonnull JoinOp op, @Nonnull RefSet<Op> shared);
}
