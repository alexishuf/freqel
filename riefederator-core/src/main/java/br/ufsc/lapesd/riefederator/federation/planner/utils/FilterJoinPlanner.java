package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.inject.ImplementedBy;

import javax.annotation.Nonnull;
import java.util.Set;

@ImplementedBy(DefaultFilterJoinPlanner.class)
public interface FilterJoinPlanner {
    @Nonnull Op rewrite(@Nonnull CartesianOp op, @Nonnull Set<RefEquals<Op>> shared);
    @Nonnull Op rewrite(@Nonnull JoinOp op, @Nonnull Set<RefEquals<Op>> shared);
}
